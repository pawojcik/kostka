// An executor: the domain object that runs tasks, one per node. It has the stage
// plan, one task slot per core of its node, a block manager (shuffle files and
// broadcast values) and a memory share per running task. It acts only on commands
// from the driver and reports back with events; it never sees the driver's state, and
// reads other nodes' shuffle files only through its ports.
//
//   LaunchTask      -> the task waits in its slot until the slot's core runs it
//   a task runs     -> TaskFinished: a map task's MapStatus, or a result task's rows,
//                      and its metrics (or TaskFailed: an error, or FetchFailed)
//   RemoveShuffle   -> deletes its shuffle files of that shuffle
//   RemoveBroadcast -> frees its copy of a broadcast
//
// A task is self-contained: its operators keep state only while it runs, and a
// failed task leaves nothing behind (its shuffle file and spilled runs are dropped),
// so it can run again anywhere. Time, IO cost and where a task runs are
// infrastructure: they come from the ports.

import { ExecutionError, KostkaError, OutOfDiskError, OutOfMemoryError, internal } from "../../../core/errors.ts";
import { rowsBytes } from "../../../core/rows.ts";
import type { Row } from "../../../core/types.ts";
import type { Charged, SpilledRun, TaskContext } from "../physical/context.ts";
import { drain } from "../physical/plan.ts";
import type { StagePlan } from "../physical/stages.ts";
import { FetchFailedError, type Command, type ExecutorEvent, type MapStatus, type TaskMetrics } from "../protocol.ts";
import { BlockManager, type ShuffleFile } from "./block-manager.ts";
import type { ExecutorPorts, RowBlock, TaskClock } from "./ports.ts";
import { TaskMemory } from "./task-memory.ts";

export type LaunchTask = Extract<Command, { type: "LaunchTask" }>;

/** A task in a slot, waiting for its core. */
export interface SlotTask {
  readonly cmd: LaunchTask;
  /** When it arrived. */
  readonly at: number;
  /** Data files its scans loaded, by operator id. */
  loaded: Map<number, { rows: Row[]; decodeNs: number } | null> | null;
}

export class Executor {
  readonly id: number;
  readonly plan: StagePlan;
  readonly slots: (SlotTask | null)[];
  readonly blocks = new BlockManager();
  /** The bytes a task may hold: memoryMB / cores. */
  readonly memoryShare: number;
  /** Tasks that failed here, for the workbench. */
  failedTasks = 0;
  /** Stopped by the infrastructure: it runs nothing more. */
  gone = false;
  /** Events not sent yet, with when they happened. */
  readonly outbox: { at: number; event: ExecutorEvent }[] = [];
  private readonly ports: ExecutorPorts;
  private readonly batchRows: number;

  constructor(id: number, plan: StagePlan, slots: number, memoryShare: number, batchRows: number, ports: ExecutorPorts) {
    this.id = id;
    this.plan = plan;
    this.slots = Array.from({ length: slots }, () => null);
    this.memoryShare = memoryShare;
    this.batchRows = batchRows;
    this.ports = ports;
  }

  handle(cmd: Command, now: number): void {
    if (this.gone) return;
    switch (cmd.type) {
      case "LaunchTask": {
        if (this.slots[cmd.slot]) throw internal(`slot ${cmd.slot} of executor ${this.id} is busy`);
        this.slots[cmd.slot] = { cmd, at: now, loaded: null };
        return;
      }
      case "RemoveShuffle": {
        for (const f of this.blocks.removeShuffle(cmd.shuffle)) this.dropFile(f);
        return;
      }
      case "RemoveBroadcast": {
        for (const v of this.blocks.removeBroadcast(cmd.broadcast)) this.ports.memory.release(v.holdKey);
        return;
      }
      default: {
        const never: never = cmd;
        return never;
      }
    }
  }

  private dropFile(f: ShuffleFile): void {
    this.ports.memory.release(f.holdKey);
    this.ports.blocks.drop(f.block);
  }

  /** The tasks waiting in their slots: each slot's core can start its own. */
  waiting(): { slot: number; task: SlotTask }[] {
    if (this.gone) return [];
    return this.slots.flatMap((t, slot) => (t ? [{ slot, task: t }] : []));
  }

  /** Loads what the task's scans read (data files), before it runs. */
  async prepare(task: SlotTask): Promise<void> {
    if (task.loaded) return;
    const stage = this.plan.stage(task.cmd.stage);
    const loaded = new Map<number, { rows: Row[]; decodeNs: number } | null>();
    for (const scan of stage.scans) loaded.set(scan.id, await scan.load(task.cmd.partition));
    task.loaded = loaded;
  }

  /** Runs the task in `slot` from `start`. Returns when it ended, and the error it
   * failed with (null if it finished, or failed with FetchFailed). */
  run(slot: number, start: number): { end: number; error: Error | null } {
    const task = this.slots[slot];
    if (!task) throw internal(`slot ${slot} of executor ${this.id} has no task`);
    this.slots[slot] = null;
    const c = task.cmd;
    const stage = this.plan.stage(c.stage);
    const clock = this.ports.begin(slot, start);
    const run = new TaskRun(this, task, clock, this.ports, this.batchRows);
    const id = { tid: c.tid, stage: c.stage, stageAttempt: c.stageAttempt, partition: c.partition, attempt: c.attempt };
    try {
      if (stage.writes) stage.writes.writeMap(run, c.partition);
      else run.send(drain(stage.top.execute(run, c.partition)));
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      run.abort();
      this.failedTasks++;
      const metrics = run.metrics(clock.end(false));
      const ff = err instanceof FetchFailedError ? err.failure : null;
      this.outbox.push({ at: metrics.end, event: { type: "TaskFailed", executor: this.id, ...id, error: `${err.name}: ${err.message}`, name: err.name, fetchFailed: ff, metrics } });
      return { end: metrics.end, error: ff ? null : err };
    }
    run.close();
    const metrics = run.metrics(clock.end(true));
    this.outbox.push({ at: metrics.end, event: { type: "TaskFinished", executor: this.id, ...id, mapStatus: run.mapStatus, rows: stage.writes ? null : run.result, metrics } });
    return { end: metrics.end, error: null };
  }

  /** What it holds, for the workbench. */
  state() {
    return {
      executor: this.id,
      slots: this.slots.map((t) => (t ? { tid: t.cmd.tid, stage: t.cmd.stage, partition: t.cmd.partition, attempt: t.cmd.attempt, stage_attempt: t.cmd.stageAttempt } : null)),
      memory_share: this.memoryShare,
      failed_tasks: this.failedTasks,
      ...(this.gone ? { shuffle_files: [], broadcasts: [] } : this.blocks.state()),
    };
  }
}

/** One task as it runs: the TaskContext its operators see. What it writes is kept or
 * undone when it ends. */
class TaskRun implements TaskContext {
  readonly partition: number;
  readonly batchRows: number;
  readonly memoryShare: number;
  mapStatus: MapStatus | null = null;
  readonly result: Row[] = [];
  private readonly ex: Executor;
  private readonly task: SlotTask;
  private readonly clock: TaskClock;
  private readonly ports: ExecutorPorts;
  private readonly memory: TaskMemory;
  private readonly written: ShuffleFile[] = [];
  private readonly spills: { key: string; block: RowBlock }[] = [];
  private readonly rowsOut = new Map<number, number>();
  private readonly notes = new Map<number, string[]>();
  private readonly fetched: number[] = [];
  private inputBytes = 0;
  private inputRows = 0;
  private shuffleReadBytes = 0;
  private shuffleReadRows = 0;
  private shuffleWriteBytes = 0;
  private shuffleWriteRows = 0;
  private spillBytes = 0;
  /** The task's name in the node's memory ("S2 task 3.0"). */
  private readonly label: string;

  constructor(ex: Executor, task: SlotTask, clock: TaskClock, ports: ExecutorPorts, batchRows: number) {
    this.ex = ex;
    this.task = task;
    this.clock = clock;
    this.ports = ports;
    this.partition = task.cmd.partition;
    this.batchRows = batchRows;
    this.memoryShare = ex.memoryShare;
    const c = task.cmd;
    this.label = `S${c.stage} task ${c.partition}.${c.attempt}`;
    this.memory = new TaskMemory(ex.memoryShare, ports.memory, this, `task:${c.tid}`, this.label);
  }

  spillWrite(bytes: number): void {
    this.clock.writeDisk(bytes);
  }

  cpu(ns: number): void {
    this.clock.cpu(Math.round(ns));
  }

  cpuTaken(ns: number): void {
    this.clock.cpuTaken(ns);
  }

  readStorage(bytes: number): void {
    this.clock.readStorage(bytes);
    this.inputBytes += bytes;
  }

  loaded(op: Charged): { rows: Row[]; decodeNs: number } | null {
    return this.task.loaded?.get(op.id) ?? null;
  }

  shuffleRead(shuffle: number, partition: number): readonly Row[] {
    const input = this.task.cmd.shuffles.find((s) => s.shuffle === shuffle);
    if (!input) throw internal(`task ${this.task.cmd.tid} was not told where shuffle ${shuffle} is`);
    const out: Row[] = [];
    input.maps.forEach((m, map) => {
      if (!m) throw new FetchFailedError({ shuffle, map, node: null });
      const bytes = m.bytes[partition] ?? 0;
      const from = m.offsets[partition] ?? 0, to = m.offsets[partition + 1] ?? from;
      if (to === from) return; // the index says this file has nothing for the partition
      const read = this.ports.readBlock(m.node, m.block);
      if (read.kind !== "ok") throw new FetchFailedError({ shuffle, map, node: m.node });
      this.clock.readDisk(bytes);
      if (m.node !== this.ex.id) this.clock.network(bytes, "in");
      for (let i = from; i < to; i++) out.push(read.rows[i]!);
      this.shuffleReadBytes += bytes;
    });
    this.shuffleReadRows += out.length;
    return out;
  }

  shuffleWrite(shuffle: number, parts: readonly (readonly Row[])[]): void {
    const rows: Row[] = [];
    const offsets: number[] = [0];
    const bytes: number[] = [];
    for (const p of parts) {
      for (const r of p) rows.push(r);
      offsets.push(rows.length);
      bytes.push(rowsBytes(p));
    }
    const block = this.ports.blocks.put(rows);
    const c = this.task.cmd;
    const holdKey = `shuffle:${shuffle}:${c.partition}:${c.tid}`;
    this.ports.memory.holdOnDisk(this, holdKey, "shuffle", `S${c.stage} shuffle ${shuffle} file`, block);
    const f: ShuffleFile = { shuffle, map: c.partition, tid: c.tid, block: block.id, bytes: block.bytes, holdKey };
    this.ex.blocks.addFile(f);
    this.written.push(f);
    this.mapStatus = { node: this.ex.id, block: block.id, bytes, offsets };
    this.shuffleWriteBytes += block.bytes;
    this.shuffleWriteRows += rows.length;
  }

  broadcast<T>(broadcast: number, op: Charged, build: (rows: readonly Row[]) => { value: T; bytes: number }): T {
    const have = this.ex.blocks.value(broadcast, op.id);
    if (have) {
      have.hits++;
      // The value was made by this same operator's `build`, so it is a T.
      return have.value as T;
    }
    const rows = this.ports.fetchBroadcast(broadcast);
    this.clock.network(rowsBytes(rows), "in");
    const built = build(rows);
    const holdKey = `broadcast:${broadcast}:op${op.id}`;
    this.ports.memory.hold(this, holdKey, "broadcast", `broadcast ${broadcast}`, built.bytes, null,
      "A broadcast's hash table is held whole on every executor: give each node more memory, or lower broadcastThreshold so that this join shuffles instead.");
    this.ex.blocks.addValue({ broadcast, op: op.id, value: built.value, bytes: built.bytes, holdKey, hits: 0 });
    this.fetched.push(broadcast);
    return built.value;
  }

  send(rows: readonly Row[]): void {
    this.clock.network(rowsBytes(rows), "out");
    for (const r of rows) this.result.push(r);
  }

  reserve(op: Charged, label: string, bytes: number): boolean {
    return this.memory.reserve(op.id, label, bytes);
  }

  free(op: Charged): void {
    this.memory.free(op.id);
  }

  spill(rows: readonly Row[]): SpilledRun {
    const block = this.ports.blocks.put(rows);
    const c = this.task.cmd;
    const key = `spill:${c.tid}:${this.spills.length}`;
    this.ports.memory.holdOnDisk(this, key, "spill", `${this.label} spilled run`, block);
    this.spills.push({ key, block });
    this.spillBytes += block.bytes;
    return { id: this.spills.length - 1, rows: rows.length, bytes: block.bytes };
  }

  unspill(run: SpilledRun): readonly Row[] {
    const s = this.spills[run.id];
    if (!s) throw internal(`task ${this.task.cmd.tid} has no spilled run ${run.id}`);
    this.clock.readDisk(run.bytes);
    return s.block.rows;
  }

  output(op: Charged, rows: number): void {
    this.rowsOut.set(op.id, (this.rowsOut.get(op.id) ?? 0) + rows);
    if (this.ex.plan.stage(this.task.cmd.stage).scans.some((s) => s.id === op.id)) this.inputRows += rows;
  }

  note(op: Charged, text: string): void {
    const list = this.notes.get(op.id) ?? [];
    if (list.length < 4) list.push(text);
    this.notes.set(op.id, list);
  }

  /** The task is done: its memory and spilled runs are freed; its shuffle file stays. */
  close(): void {
    this.memory.close();
    for (const s of this.spills) {
      this.ports.memory.release(s.key);
      this.ports.blocks.drop(s.block.id);
    }
  }

  /** The task failed: undo it, so it can run again from scratch. */
  abort(): void {
    this.close();
    for (const f of this.written) {
      this.ex.blocks.dropFile(f);
      this.ports.memory.release(f.holdKey);
      this.ports.blocks.drop(f.block);
    }
    this.mapStatus = null;
    this.result.length = 0;
  }

  metrics(t: ReturnType<TaskClock["end"]>): TaskMetrics {
    const stage = this.ex.plan.stage(this.task.cmd.stage);
    return {
      start: t.start, end: t.end, time: t.time, segments: t.segments,
      inputBytes: this.inputBytes, inputRows: this.inputRows, shuffleReadBytes: this.shuffleReadBytes, shuffleReadRows: this.shuffleReadRows,
      shuffleWriteBytes: this.shuffleWriteBytes, shuffleWriteRows: this.shuffleWriteRows, spillBytes: this.spillBytes, peakMemory: this.memory.peak,
      ops: stage.ops.map((op) => ({ op: op.id, rows: this.rowsOut.get(op.id) ?? 0, notes: this.notes.get(op.id) ?? [] })).concat(
        stage.writes ? [{ op: stage.writes.id, rows: this.shuffleWriteRows, notes: this.notes.get(stage.writes.id) ?? [] }] : []),
      fetched: this.fetched,
    };
  }
}

/** The error a query that failed in its tasks throws: of the kind the last task error
 * was, with the driver's message. A bug (not a KostkaError) is thrown as it is. */
export function taskFailure(last: Error | null, message: string): Error {
  if (last && !(last instanceof KostkaError) && !(last instanceof FetchFailedError)) return last;
  const cause = last ? { cause: last } : undefined;
  if (last instanceof OutOfMemoryError) return new OutOfMemoryError(message, cause);
  if (last instanceof OutOfDiskError) return new OutOfDiskError(message, cause);
  return new ExecutionError(message, cause);
}
