// The Spark-style engine on the simulated cluster: the driver and the executors (the
// domain) wired to the shared infrastructure (cluster/). This file is the adapter
// between them; it decides nothing about the query.
//
// - Nodes: executor i is on node i, with a task slot per core: slot s runs its task
//   on core s, so tasks on one node run side by side, sharing the node's IO channel.
//   It uses the node's memory, disk and block store.
// - Transport and executor: the shared event loop (cluster/loop.ts). Commands reach
//   their executor at once; an executor's events go through the loop's queue, so the
//   driver handles them in the order they happened. A task runs atomically, laid out
//   on its core's timeline by its clock (SimTaskClock).
// - Data plane: a task reads a shuffle file (a block) from the node holding it; the
//   driver's broadcasts are fetched over the network.
// - Failures: `stopNode(n)` stops node n just before the next thing happens. The
//   events its executor sent that were not delivered yet are lost with it, its shuffle
//   files are gone, and the driver gets ExecutorLost.
//
// It also keeps the history: every message, grouped into steps (a driver decision
// with the event it handled and the commands it sent; a task an executor ran with the
// event it sent), and every node's frame after each.

import { ExecutionError, internal } from "../../core/errors.ts";
import { CATEGORIES, type Category } from "../../core/time.ts";
import type { Row } from "../../core/types.ts";
import type { PlanNode } from "../../sql/plan.ts";
import { BlockLostError, BlockStore, DataPlane } from "../../cluster/blocks.ts";
import { IO_PROFILES, checkCluster, type ClusterConfig } from "../../cluster/config.ts";
import { EventLoop, History, type LoggedMessage, type Runnable } from "../../cluster/loop.ts";
import { MemoryPool } from "../../cluster/memory.ts";
import { Node, type NodeFrame, type Unit } from "../../cluster/node.ts";
import type { ExecEvent, ExecState, Execution } from "../../execution/engine.ts";
import { DAGScheduler, MAX_STAGE_ATTEMPTS, type Decision } from "./driver/dag-scheduler.ts";
import type { SparkSettings } from "./engine.ts";
import { Executor, taskFailure, type SlotTask } from "./executor/executor.ts";
import type { BlockRead, TaskClock } from "./executor/ports.ts";
import { partitionCount } from "./physical/plan.ts";
import type { SparkPlan } from "./physical/planner.ts";
import type { StagePlan } from "./physical/stages.ts";
import { describeMessage, taskName, type Command, type DriverEvent, type ExecutorEvent } from "./protocol.ts";

/** A task's time on its core, in the cluster's terms. */
class SimTaskClock implements TaskClock {
  readonly measured: boolean;
  private readonly unit: Unit;
  private mark = 0;
  constructor(unit: Unit, measured: boolean) {
    this.unit = unit;
    this.measured = measured;
    if (measured) this.mark = performance.now();
  }
  /** Measured mode: the time since the last event is the task's CPU time. */
  private lap(): void {
    if (!this.measured) return;
    const now = performance.now();
    this.unit.cpu(Math.round((now - this.mark) * 1e6));
    this.mark = now;
  }
  cpu(ns: number): void {
    if (!this.measured) this.unit.cpu(ns);
  }
  cpuTaken(ns: number): void {
    if (!this.measured) return;
    this.lap();
    this.unit.cpu(ns);
  }
  readStorage(bytes: number): void {
    this.lap();
    this.unit.io("storage", bytes, "storage_read");
  }
  writeDisk(bytes: number): void {
    this.lap();
    this.unit.io("spill_write", bytes, "spill_written");
  }
  readDisk(bytes: number): void {
    this.lap();
    this.unit.io("spill_read", bytes, "spill_read");
  }
  network(bytes: number, dir: "in" | "out"): void {
    this.lap();
    this.unit.io("network", bytes, dir === "in" ? "network_in" : "network_out");
  }
  end(finished: boolean) {
    this.lap();
    const t = this.unit.end(finished);
    return { start: t.start, end: t.end, time: t.time, segments: t.segments };
  }
}

/** A task an executor could start: in its slot, on that core. */
interface QueuedTask extends Runnable {
  readonly executor: Executor;
  readonly slot: number;
  readonly task: SlotTask;
}

export interface UnitRecord {
  readonly id: number;
  readonly tid: number;
  readonly stage: number;
  readonly stageAttempt: number;
  readonly partition: number;
  readonly attempt: number;
  readonly node: number;
  readonly slot: number;
  readonly start: number;
  readonly end: number;
  readonly failed: string | null;
  readonly segments: readonly (readonly [Category | "wait", number, number])[];
  readonly time: Readonly<Record<Category, number>>;
  readonly seq: number;
}

type SparkNodeFrame = NodeFrame & ReturnType<Executor["state"]>;

const sum = (xs: readonly number[]) => xs.reduce((a, b) => a + b, 0);

export class SparkExecution implements Execution {
  readonly engine = "spark";
  readonly plan: SparkPlan;
  readonly stages: StagePlan;
  readonly cluster: ClusterConfig;
  readonly settings: SparkSettings;
  readonly driver: DAGScheduler;
  readonly executors: Executor[];
  readonly nodes: Node[];
  readonly units: UnitRecord[] = [];
  private readonly loop: EventLoop<DriverEvent, QueuedTask>;
  private readonly history: History<SparkNodeFrame>;
  private readonly plane: DataPlane;
  private readonly planIds: ReadonlyMap<PlanNode, number>;
  /** The event the driver is handling: its first decision logs it. */
  private handling: { event: DriverEvent; at: number; extra: LoggedMessage[] } | null = null;
  /** The error the latest failed task threw: a query failed in its tasks throws it. */
  private lastError: Error | null = null;
  private thrown = false;

  constructor(plan: SparkPlan, stages: StagePlan, cluster: ClusterConfig, settings: SparkSettings, planIds: ReadonlyMap<PlanNode, number>) {
    this.plan = plan;
    this.stages = stages;
    this.cluster = checkCluster(cluster);
    this.settings = settings;
    this.planIds = planIds;
    const profile = IO_PROFILES[cluster.io];
    if (!profile) throw internal(`checkCluster let through IO profile ${cluster.io}`);
    const capacity = Math.round(cluster.memoryMB * 1024 * 1024), disk = Math.round(cluster.diskMB * 1024 * 1024);
    this.nodes = Array.from({ length: cluster.nodes }, (_, i) =>
      new Node(i, cluster.cores, profile, new MemoryPool(`executor ${i}`, capacity, disk, "Give each node more disk, or more memory."), new BlockStore(i)));
    this.plane = new DataPlane(this.nodes);
    const measured = cluster.cpu === "measured";
    const share = Math.floor(capacity / cluster.cores);
    this.executors = this.nodes.map((n) => new Executor(n.id, stages, cluster.cores, share, cluster.batchRows, {
      begin: (slot, start) => new SimTaskClock(n.begin(slot, start), measured),
      readBlock: (node, block) => this.readBlock(node, block),
      fetchBroadcast: (b) => this.driver.broadcast(b),
      memory: n.memory,
      blocks: n.blocks,
    }));
    this.loop = new EventLoop(this.nodes, {
      deliver: (event, at) => {
        this.handling = { event, at, extra: [] };
        this.driver.on(event, at);
        this.handling = null;
      },
      runnable: () => this.executors.flatMap((e) => e.waiting().map(({ slot, task }) => ({ node: e.id, core: slot, arrived: task.at, executor: e, slot, task }))),
      run: (u, start) => this.runTask(u, start),
    });
    this.history = new History(this.nodes, (n) => ({ ...n.frame(), ...this.executorOf(n.id).state() }));
    this.driver = new DAGScheduler(stages, { executors: cluster.nodes, slots: cluster.cores, maxTaskFailures: settings.maxTaskFailures, maxStageAttempts: MAX_STAGE_ATTEMPTS }, {
      decide: (d, now) => this.decided(d, now),
      seq: () => this.events.length,
    });
    this.driver.start(0);
  }

  get events(): readonly ExecEvent[] {
    return this.history.steps;
  }

  private executorOf(i: number): Executor {
    const e = this.executors[i];
    if (!e) throw internal(`there is no executor ${i}`);
    return e;
  }

  private readBlock(node: number, block: number): BlockRead {
    try {
      const b = this.plane.lookup(node, block);
      return b ? { kind: "ok", rows: b.rows } : { kind: "missing" };
    } catch (e) {
      // the executor's view of a block gone with its node
      if (e instanceof BlockLostError) return { kind: "gone" };
      throw e;
    }
  }

  explain(): string {
    const stages = this.stages.stages.map((s) => `Stage ${s.id} (job ${s.job}, ${s.kind === "map" ? "ShuffleMapStage" : "ResultStage"}, ${s.partitions} task(s)${s.parents.length ? `, after stage ${s.parents.join(", ")}` : ""}): ${s.ops.map((o) => o.name()).join(" ← ")}`);
    return `${this.plan.explain()}\n\n${stages.join("\n")}`;
  }

  // --- the transport and the history ------------------------------------------------

  private log(m: Command | DriverEvent, dir: LoggedMessage["dir"], at: number): LoggedMessage {
    const text = describeMessage(m);
    if (m.type === "ExecutorLost") return { dir, type: m.type, from: "simulation", to: "driver", ms: at / 1e6, text };
    const e = `E${m.executor}`;
    const toExecutor = m.type === "LaunchTask" || m.type === "RemoveShuffle" || m.type === "RemoveBroadcast";
    return { dir, type: m.type, from: toExecutor ? "driver" : e, to: toExecutor ? e : "driver", ms: at / 1e6, text };
  }

  /** Takes what an executor sent: logged in the step, queued for the driver. */
  private drain(e: Executor, into: LoggedMessage[]): ExecutorEvent[] {
    const out: ExecutorEvent[] = [];
    for (const { at, event } of e.outbox.splice(0)) {
      into.push(this.log(event, "event", at));
      this.loop.send(at, e.id, event);
      out.push(event);
    }
    return out;
  }

  /** A driver decision: the event it handled, and its commands, which reach the
   * executors now; all of it is one step. */
  private decided(d: Decision, now: number): void {
    const msgs: LoggedMessage[] = [];
    const touched = new Set<number>();
    if (this.handling) {
      msgs.push(this.log(this.handling.event, "event", this.handling.at), ...this.handling.extra);
      touched.add(this.handling.event.executor);
      this.handling = null;
    }
    for (const c of d.commands) {
      msgs.push(this.log(c, "command", now));
      this.executorOf(c.executor).handle(c, now);
      touched.add(c.executor);
    }
    const refs: Record<string, number> = {};
    for (const [k, v] of Object.entries(d.refs)) if (typeof v === "number") refs[k] = v;
    this.history.record({ ms: now / 1e6, kind: d.kind, text: d.text, node: d.refs.executor ?? null, refs, messages: msgs }, touched);
  }

  // --- the executor -----------------------------------------------------------------

  private async runTask(u: QueuedTask, start: number): Promise<void> {
    const e = u.executor;
    await e.prepare(u.task);
    const c = u.task.cmd;
    const { error } = e.run(u.slot, start);
    if (error) this.lastError = error;
    const msgs: LoggedMessage[] = [];
    const out = this.drain(e, msgs);
    const ev = out[0];
    if (!ev || out.length !== 1) throw internal(`executor ${e.id} ran ${taskName(c)} and sent ${out.length} events`);
    const m = ev.metrics;
    if (!m) throw internal(`${taskName(c)} reported no metrics`);
    const failed = ev.type === "TaskFailed" ? (ev.fetchFailed ? `FetchFailed: ${ev.error}` : ev.error) : null;
    this.units.push({
      id: this.units.length, tid: c.tid, stage: c.stage, stageAttempt: c.stageAttempt, partition: c.partition, attempt: c.attempt,
      node: e.id, slot: u.slot, start: m.start, end: m.end, failed, segments: m.segments, time: m.time, seq: this.events.length,
    });
    const io = (m.time.storage + m.time.network + m.time.spill_read + m.time.spill_write) / 1e6;
    const cost = `${(m.time.cpu / 1e6).toFixed(2)} ms CPU, ${io.toFixed(2)} ms IO`;
    const n = (x: number) => x.toLocaleString("en");
    const did = [
      m.inputRows ? `read ${n(m.inputRows)} row(s) from storage` : "",
      m.shuffleReadRows ? `read ${n(m.shuffleReadRows)} shuffled row(s)` : "",
      m.fetched.length ? `fetched broadcast ${m.fetched.join(", ")} and built it` : "",
      m.shuffleWriteRows ? `wrote a shuffle file of ${n(m.shuffleWriteRows)} row(s)` : "",
      m.spillBytes ? `spilled ${n(m.spillBytes)} bytes` : "",
      ev.type === "TaskFinished" && ev.rows ? `${n(ev.rows.length)} row(s) for the driver` : "",
    ].filter(Boolean);
    const text = failed
      ? `executor ${e.id} ran ${taskName(c)} in slot ${u.slot}, and it failed: ${failed}`
      : `executor ${e.id} ran ${taskName(c)} in slot ${u.slot}: ${did.join("; ") || "no rows"}; ${cost}; TaskFinished`;
    this.history.record({ ms: m.end / 1e6, kind: "TaskRan", text, node: e.id, refs: { stage: c.stage, tid: c.tid, partition: c.partition, executor: e.id }, messages: msgs }, new Set([e.id]));
  }

  /** A failure: node `n` (and its executor) stops just before the next thing happens. */
  stopNode(n: number): void {
    if (this.done || this.driver.failed) throw new ExecutionError("the query is over");
    const node = this.nodes[n];
    if (!node) throw new RangeError(`there is no node ${n}`);
    if (!node.up) throw new RangeError(`node ${n} is gone already`);
    const last = this.events[this.events.length - 1];
    const { at, lost } = this.loop.stop(n, last ? last.ms * 1e6 : 0);
    this.executorOf(n).gone = true;
    const extra = lost.map((q) => ({ ...this.log(q.event, "lost", q.at), text: `lost with executor ${n}: ${describeMessage(q.event)}` }));
    const event: DriverEvent = { type: "ExecutorLost", executor: n };
    this.handling = { event, at, extra };
    this.driver.on(event, at);
    this.handling = null;
  }

  private check(): void {
    const f = this.driver.failed;
    if (!f) return;
    const message = `the query failed in ${f.unit}: ${f.error}`;
    if (!this.thrown) {
      this.thrown = true;
      throw f.cause === "task" ? taskFailure(this.lastError, message) : new ExecutionError(message);
    }
    throw new ExecutionError(message);
  }

  get done(): boolean {
    return this.driver.done;
  }

  get elapsedNs(): number {
    return this.loop.elapsed;
  }

  async step(): Promise<boolean> {
    this.check();
    if (this.done) return false;
    const before = this.events.length;
    while (this.events.length === before) {
      if (!(await this.loop.advance())) throw internal("the query is stuck: nothing can happen");
    }
    this.check();
    return !this.done;
  }

  async wave(): Promise<number> {
    const used = new Set<string>();
    let ran = 0;
    for (;;) {
      this.check();
      if (this.done) break;
      const a = this.loop.peek();
      if (!a) break;
      if (a.kind === "unit") {
        const k = `${a.unit.node}:${a.unit.core}`;
        if (used.has(k)) break;
        used.add(k);
        ran++;
      }
      await this.loop.advance();
    }
    this.check();
    return ran;
  }

  /** Steps until the stages running now are done. */
  async runConcurrent(onProgress?: () => void): Promise<void> {
    const target = this.driver.stages.filter((s) => s.status.now === "running");
    while (!this.done && !this.driver.failed && target.some((s) => s.status.now !== "done")) {
      await this.step();
      onProgress?.();
    }
  }

  async run(onProgress?: () => void): Promise<Row[]> {
    while (await this.step()) onProgress?.();
    return this.result();
  }

  result(): Row[] {
    this.check();
    if (!this.done) throw new ExecutionError("the query has not finished; step or run it");
    return this.driver.rows();
  }

  rows(): readonly Row[] {
    return this.driver.rows();
  }

  progress(): { done: number; total: number } {
    const total = sum(this.stages.stages.map((s) => s.partitions));
    const done = sum(this.driver.stages.map((s) => (s.status.now === "done" ? s.stage.partitions : 0)));
    return { done, total };
  }

  // --- the workbench's view ---------------------------------------------------------

  private nextAfter(at: number): ExecState["next"] {
    for (let i = at + 1; i < this.events.length; i++) {
      const e = this.events[i];
      if (e && e.kind === "TaskRan") return { node: e.node, kind: "task", label: `stage ${e.refs.stage}, partition ${e.refs.partition}`, refs: e.refs };
    }
    const a = this.loop.peek();
    if (!a) return null;
    if (a.kind === "deliver") return { node: null, kind: "event", label: describeMessage(a.event), refs: {} };
    const c = a.unit.task.cmd;
    return { node: a.unit.node, kind: "task", label: taskName(c), refs: { stage: c.stage, tid: c.tid, partition: c.partition } };
  }

  state(at = this.events.length - 1): ExecState {
    at = Math.max(0, Math.min(at, this.events.length - 1));
    const ev = this.events[at];
    if (!ev) throw internal("an execution always has its first step");
    const d = this.driver;
    const nodes = this.history.framesAt(at);
    const done = this.events.slice(0, at + 1).some((e) => e.kind === "SQLExecutionEnd");
    const failed = d.failed && d.failed.seq <= at ? { unit: d.failed.unit, error: d.failed.error } : null;
    const units = this.units.filter((u) => u.seq <= at);
    const tasks = d.tasks.filter((t) => t.seq <= at).map((t) => {
      const status = t.status.after(at) ?? "running";
      const m = status === "running" ? null : t.metrics;
      const ran = units.find((u) => u.tid === t.tid);
      return {
        tid: t.tid, stage: t.stage, stage_attempt: t.stageAttempt, partition: t.partition, attempt: t.attempt, executor: t.executor, slot: t.slot,
        status, launched_ms: t.at / 1e6, ran: !!ran, start_ms: ran ? ran.start / 1e6 : null, end_ms: ran ? ran.end / 1e6 : null,
        error: status === "failed" || status === "lost" ? t.error : null, fetch_failed: status === "failed" && t.fetchFailed,
        input_bytes: m?.inputBytes ?? 0, shuffle_read_bytes: m?.shuffleReadBytes ?? 0, shuffle_write_bytes: m?.shuffleWriteBytes ?? 0,
        spill_bytes: m?.spillBytes ?? 0, peak_memory: m?.peakMemory ?? 0,
        time_ms: Object.fromEntries(CATEGORIES.map((c) => [c, (m?.time[c] ?? 0) / 1e6])),
      };
    });
    // Rows each operator gave out: from the latest successful task of each partition.
    const opRows = new Map<number, number>();
    const latest = new Map<string, (typeof d.tasks)[number]>();
    for (const t of d.tasks) if (t.seq <= at && t.status.after(at) === "success") latest.set(`${t.stage}:${t.partition}`, t);
    for (const t of latest.values()) for (const o of t.metrics?.ops ?? []) opRows.set(o.op, (opRows.get(o.op) ?? 0) + o.rows);
    const stageOf = new Map<number, number>();
    for (const s of this.stages.stages) for (const o of s.ops) stageOf.set(o.id, s.id);
    for (const s of this.stages.stages) if (s.writes) stageOf.set(s.writes.id, s.id);
    const stages = d.stages.map((s) => {
      const st = s.stage;
      const ts = tasks.filter((t) => t.stage === st.id);
      const attempt = s.attempt.after(at) ?? -1;
      const cur = ts.filter((t) => t.stage_attempt === attempt);
      const ok = ts.filter((t) => t.status === "success");
      const status = s.status.after(at) ?? "waiting";
      const runs = units.filter((u) => u.stage === st.id);
      return {
        id: st.id, job: st.job, kind: st.kind, status, why: s.why.after(at) ?? "", attempt, partitions: st.partitions, parents: st.parents,
        reads: st.reads, writes: st.writes?.shuffle ?? null, broadcasts: st.broadcasts, ops: st.ops.map((o) => o.id), top: st.top.id, exchange: st.writes?.id ?? null,
        attempts: s.attempts.filter((a) => a.seq <= at).map((a) => ({ attempt: a.attempt, tasks: a.partitions.length, submitted_ms: a.at / 1e6 })),
        tasks: { total: attempt < 0 ? st.partitions : (s.attempts.find((a) => a.attempt === attempt)?.partitions.length ?? 0), done: cur.filter((t) => t.status === "success").length, running: cur.filter((t) => t.status === "running").length, failed: ts.filter((t) => t.status === "failed").length },
        input_bytes: sum(ok.map((t) => t.input_bytes)), shuffle_read_bytes: sum(ok.map((t) => t.shuffle_read_bytes)), shuffle_write_bytes: sum(ok.map((t) => t.shuffle_write_bytes)), spill_bytes: sum(ok.map((t) => t.spill_bytes)),
        start_ms: runs.length ? Math.min(...runs.map((u) => u.start)) / 1e6 : null, end_ms: status === "done" && runs.length ? Math.max(...runs.map((u) => u.end)) / 1e6 : null,
      };
    });
    const jobs = this.stages.jobs.map((j) => {
      const js = d.jobs[j.id]!; // jobs are made from the plan's jobs
      return {
        id: j.id, broadcast: j.broadcast?.broadcast ?? null, status: js.status.after(at) ?? "waiting", stages: j.stages, needs: j.needs,
        stages_done: stages.filter((s) => s.job === j.id && s.status === "done").length,
      };
    });
    const ops = this.plan.ops.map((o) => ({
      id: o.id, name: o.name(), args: o.args(), label: o.label(), children: o.children().map((c) => c.id), stage: stageOf.get(o.id) ?? null,
      partitioning: partitionCount(o.partitioning()), node: o.planNode ? (this.planIds.get(o.planNode) ?? null) : null, rows: opRows.get(o.id) ?? 0,
      exchange: o.name() === "Exchange" ? "shuffle" : o.name() === "BroadcastExchange" ? "broadcast" : null,
      shuffle: this.plan.shuffles.find((x) => x === o)?.shuffle ?? null, broadcast: this.plan.broadcasts.find((x) => x === o)?.broadcast ?? null,
    }));
    const shuffles = this.plan.shuffles.map((x) => ({
      shuffle: x.shuffle, writer: this.stages.writer.get(x.shuffle) ?? null, partitions: x.n,
      outputs: d.tracker.at(x.shuffle, at).map((o, map) => (o ? { map, node: o.status.node, bytes: sum(o.status.bytes), state: o.state, tid: o.tid } : { map, node: null, bytes: 0, state: "missing", tid: null })),
    }));
    const elapsed = Math.max(ev.ms, ...nodes.map((w) => w.clock_ms));
    return {
      engine: "spark", at, events: this.events.length, event: ev, done, failed, elapsed_ms: elapsed, next: done || failed ? null : this.nextAfter(at),
      cluster: this.cluster, settings: { ...this.settings, shuffle_partitions: this.plan.settings.shufflePartitions, memory_share: Math.floor((this.cluster.memoryMB * 1024 * 1024) / this.cluster.cores) },
      nodes: nodes.map((f) => ({ ...f, executor_status: d.executors[f.node]?.after(at) ?? "up", running: tasks.filter((t) => t.executor === f.node && t.status === "running").map((t) => ({ tid: t.tid, slot: t.slot, stage: t.stage, partition: t.partition, attempt: t.attempt, ran: t.ran })) })),
      units: units.map((u) => ({
        id: u.id, node: u.node, core: u.slot, start_ms: u.start / 1e6, end_ms: u.end / 1e6, label: `stage ${u.stage}.${u.stageAttempt}, task ${u.partition}.${u.attempt}`, group: `S${u.stage}`, failed: u.failed,
        segments: u.segments.map(([k, a, b]) => [k, a / 1e6, b / 1e6] as const), tid: u.tid, stage: u.stage, stage_attempt: u.stageAttempt, partition: u.partition, attempt: u.attempt,
        time_ms: Object.fromEntries(CATEGORIES.map((c) => [c, (u.time[c] ?? 0) / 1e6])), seq: u.seq,
      })),
      units_run: units.length, result_rows: this.resultRowsAt(tasks),
      ops, joins: this.plan.joins, jobs, stages, tasks, shuffles,
      log: this.events.slice(Math.max(0, at - 199), at + 1).map((e) => [e.ms, e.text, e.kind, e.seq]),
    };
  }

  /** The result rows in after a step: each result partition once, whichever task
   * reported it first. */
  private resultRowsAt(tasks: readonly { readonly stage: number; readonly partition: number; readonly status: string }[]): number {
    const r = this.stages.resultStage(this.stages.resultJob.id).id;
    const parts = new Set(tasks.filter((t) => t.stage === r && t.status === "success").map((t) => t.partition));
    return sum([...parts].map((p) => this.driver.stage(r).results.get(p)?.length ?? 0));
  }
}
