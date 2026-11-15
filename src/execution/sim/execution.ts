// The Execution: a Coordinator and Workers (the domain) on a simulated
// cluster (this file and run-context.ts: the infrastructure).
//
// - Transport: commands reach their worker at once; events go through a queue
//   ordered by simulated time, so the coordinator handles them in the order they
//   happened.
// - Executor and clock: one thread per worker. A worker runs a split when it has
//   one, from the later of its clock and the time the split arrived. The simulation
//   always advances the earliest thing that can happen: delivering an event, or
//   starting a split (an event first on a tie). A split runs atomically, placed on
//   its worker's timeline by its RunContext.
// - Data plane: a worker reads a piece from the worker holding it.
// - Failures: `stopWorker(w)` stops a worker just before the next thing happens. The
//   events it sent that were not delivered yet are lost with it, its pieces are
//   gone (reading one fails with PieceLostError), and the coordinator gets
//   WorkerGone.
//
// It also keeps the history: every message, grouped into steps (a coordinator
// decision with the event it handled and the commands it sent; a split a worker ran
// with the event it sent), and for each step what changed on the workers (a frame
// that shares what the step did not touch with the frame before). The coordinator's
// QueryState keeps its own history, so `state(at)` puts the two together.

import { ExecutionError, internal } from "../../core/errors.ts";
import { toJson } from "../../core/json.ts";
import type { Row } from "../../core/types.ts";
import { outputKey, type PieceRef } from "../../physical/buffer.ts";
import { CATEGORIES } from "../../physical/context.ts";
import type { OperatorInstance, SourceInstance } from "../../physical/operator.ts";
import type { PhysicalPlan } from "../../physical/pipeline.ts";
import { IO_PROFILES, checkCluster, type ClusterConfig } from "../cluster.ts";
import { Coordinator, type Decision, type Refs, type StepKind } from "../coordinator/coordinator.ts";
import type { SplitState } from "../coordinator/state.ts";
import { PieceLostError, describeMessage, type Command, type CoordinatorEvent, type UnitReport, type WorkerEvent } from "../protocol.ts";
import type { ExecEvent, Execution, LoggedMessage } from "../runtime.ts";
import { WorkerNode, type SplitHooks, type Work } from "../worker/worker.ts";
import { Machine, RunContext, timed, timedSource } from "./run-context.ts";

export interface UnitRecord {
  id: number;
  pipeline: number;
  worker: number;
  split: number;
  attempt: number;
  label: string;
  report: UnitReport | null;
  /** Why it failed, if it did. */
  failed: string | null;
  start: number;
  end: number;
  /** The step it ran in. */
  seq: number;
}

const plural = (n: number, word: string) => `${n.toLocaleString("en")} ${word}${n === 1 ? "" : "s"}`;
const msOf = (ns: number | null) => (ns === null ? null : ns / 1e6);

export class SimExecution implements Execution {
  readonly plan: PhysicalPlan;
  readonly cluster: ClusterConfig;
  readonly coordinator: Coordinator;
  readonly workers: WorkerNode[];
  readonly machines: Machine[];
  readonly events: ExecEvent[] = [];
  readonly units: UnitRecord[] = [];
  /** Per step, each worker's state (shared with the step before when untouched). */
  private readonly frames: Record<string, unknown>[][] = [];
  private readonly queue: { at: number; n: number; event: CoordinatorEvent }[] = [];
  private sent = 0;
  /** The event the coordinator is handling: its decisions log it. */
  private handling: { event: CoordinatorEvent; at: number; extra: LoggedMessage[] } | null = null;
  /** The error a failed split threw: the step that records the failure throws it. */
  private error: Error | null = null;
  private thrown = false;

  constructor(plan: PhysicalPlan, cluster: ClusterConfig) {
    this.plan = plan;
    this.cluster = checkCluster(cluster);
    const profile = IO_PROFILES[cluster.io]!;
    this.machines = Array.from({ length: cluster.workers }, (_, i) => new Machine(i, profile));
    const measured = cluster.cpu === "measured";
    const ports = {
      begin: (w: WorkerNode, work: Work, hooks: SplitHooks, start: number) =>
        new RunContext(this.machines[w.id]!, work, hooks, start, cluster.batchRows, cluster.cpu, (ref) => this.fetch(ref)),
      instrument: measured ? timed : (_: unknown, i: OperatorInstance) => i,
      instrumentSource: measured ? timedSource : (_: unknown, i: SourceInstance) => i,
    };
    const capacity = Math.round(cluster.memoryMB * 1024 * 1024), disk = Math.round(cluster.diskMB * 1024 * 1024);
    this.workers = Array.from({ length: cluster.workers }, (_, i) => new WorkerNode(i, capacity, plan, ports, disk));
    this.coordinator = new Coordinator(plan, { workers: cluster.workers, splitRows: cluster.splitRows, queueDepth: 2 }, {
      decide: (d, now) => this.decided(d, now),
      seq: () => this.events.length,
    });
    this.coordinator.start(0);
  }

  /** The data plane: the holder's copy of a piece. */
  private fetch(ref: PieceRef) {
    const holder = this.workers[ref.worker]!;
    if (holder.gone) throw new PieceLostError(ref);
    const p = holder.buffers.get(ref.id);
    if (!p) throw internal(`W${ref.worker} no longer holds piece ${ref.id} of B${ref.buffer}`);
    return p;
  }

  // --- the transport and the history ------------------------------------------------

  private log(m: Command | CoordinatorEvent, dir: LoggedMessage["dir"], at: number): LoggedMessage {
    if (m.type === "WorkerGone") return { dir, type: m.type, from: "simulation", to: "coordinator", ms: at / 1e6, text: describeMessage(m) };
    const w = `W${m.worker}`;
    const toWorker = m.type === "AddSplits" || m.type === "ReleaseBuffer";
    return { dir, type: m.type, from: toWorker ? "coordinator" : w, to: toWorker ? w : "coordinator", ms: at / 1e6, text: describeMessage(m) };
  }

  /** Takes what a worker sent: logged in the step, queued for the coordinator. */
  private drain(w: WorkerNode, into: LoggedMessage[]): WorkerEvent[] {
    const out: WorkerEvent[] = [];
    for (const { at, event } of w.outbox.splice(0)) {
      into.push(this.log(event, "event", at));
      this.queue.push({ at, n: this.sent++, event });
      out.push(event);
    }
    this.queue.sort((a, b) => a.at - b.at || a.n - b.n);
    return out;
  }

  /** A coordinator decision: the event it handled, and its commands, which reach the
   * workers now; all of it is one step. */
  private decided(d: Decision, now: number): void {
    const msgs: LoggedMessage[] = [];
    const touched = new Set<number>();
    if (this.handling) {
      msgs.push(this.log(this.handling.event, "event", this.handling.at), ...this.handling.extra);
      if (this.handling.event.type === "WorkerGone") touched.add(this.handling.event.worker);
      this.handling = null;
    }
    for (const c of d.commands) {
      msgs.push(this.log(c, "command", now));
      const w = this.workers[c.worker]!;
      w.handle(c, now);
      touched.add(c.worker);
      this.drain(w, msgs);
    }
    this.record(d.kind, d.text, now, d.refs, msgs, touched);
  }

  private record(kind: StepKind, text: string, now: number, r: Refs, messages: LoggedMessage[], touched: Set<number>): void {
    const seq = this.events.length;
    this.events.push({
      seq, ms: now / 1e6, kind, text, worker: r.worker ?? null, pipeline: r.pipeline ?? null,
      split: r.split ?? null, buffer: r.buffer ?? null, partition: r.partition ?? null, messages,
    });
    const prev = this.frames[this.frames.length - 1];
    // a worker gone since the last frame is drawn again: it lost its queues and cache
    this.frames.push(this.workers.map((w, i) => (!prev || touched.has(i) || (w.gone && !prev[i]!.gone) ? this.workerFrame(w) : prev[i]!)));
  }

  private workerFrame(w: WorkerNode): Record<string, unknown> {
    const m = this.machines[w.id]!;
    const gone = w.gone ? { queued: [], cache: [] } : {};
    return {
      ...w.state(), ...gone, clock_ms: m.clock / 1e6, bytes: { ...m.bytes, spill_written: w.memory.bytesSpilled }, runs: m.runs,
      time_ms: Object.fromEntries(CATEGORIES.map((c) => [c, m.time[c] / 1e6])), busy_ms: m.busy / 1e6,
    };
  }

  // --- the executor ---------------------------------------------------------------

  /** The earliest thing that can happen: an event to deliver, or a split to start. */
  private peekAction(): { kind: "deliver"; at: number } | { kind: "unit"; at: number; worker: WorkerNode; work: Work } | null {
    let unit: { kind: "unit"; at: number; worker: WorkerNode; work: Work } | null = null;
    for (const w of this.workers) {
      const work = w.next();
      if (!work) continue;
      const at = Math.max(this.machines[w.id]!.clock, work.at);
      if (!unit || at < unit.at) unit = { kind: "unit", at, worker: w, work };
    }
    const d = this.queue[0];
    if (d && (!unit || d.at <= unit.at)) return { kind: "deliver", at: d.at };
    return unit;
  }

  /** Advances the simulation by one action. False when nothing can happen. */
  private async advance(): Promise<boolean> {
    const a = this.peekAction();
    if (!a) return false;
    if (a.kind === "deliver") {
      const d = this.queue.shift()!;
      this.handling = { event: d.event, at: d.at, extra: [] };
      this.coordinator.on(d.event, d.at);
      this.handling = null;
    } else await this.runUnit(a.worker, a.work, a.at);
    return true;
  }

  private async runUnit(w: WorkerNode, work: Work, start: number): Promise<void> {
    const m = this.machines[w.id]!;
    await w.prepare(work);
    const { end, error } = w.run(work, start);
    m.clock = end;
    const msgs: LoggedMessage[] = [];
    const out = this.drain(w, msgs);
    const s = work.assigned.split;
    const done = out.find((e) => e.type === "SplitDone") as Extract<WorkerEvent, { type: "SplitDone" }> | undefined;
    const failed = out.find((e) => e.type === "SplitFailed") as Extract<WorkerEvent, { type: "SplitFailed" }> | undefined;
    if (done) {
      m.runs++;
      for (const c of CATEGORIES) m.time[c] += done.unit.time[c];
    }
    this.units.push({
      id: this.units.length, pipeline: work.pipeline.index, worker: w.id, split: s.id, attempt: work.assigned.attempt, label: s.label,
      report: done?.unit ?? null, failed: failed?.error ?? null, start, end, seq: this.events.length,
    });
    const refs = { worker: w.id, pipeline: work.pipeline.index, split: s.id, partition: s.partition ?? undefined };
    const att = work.assigned.attempt > 1 ? ` (attempt ${work.assigned.attempt})` : "";
    let text: string;
    if (failed) text = failed.lost ? `W${w.id} ran split ${s.id}${att} of P${work.pipeline.index}, but a piece it reads was on W${failed.lost.worker}, which is gone: SplitFailed` : `W${w.id} ran split ${s.id}${att} of P${work.pipeline.index}, and it failed: ${failed.error}`;
    else {
      const u = done!.unit;
      const io = (u.time.storage + u.time.network + u.time.spill_read + u.time.spill_write) / 1e6;
      const did = [...u.tables, done!.pieces.length ? `wrote ${plural(done!.pieces.length, "piece")}` : "", done!.result ? `${plural(done!.result.length, "result row")} for the coordinator` : ""].filter(Boolean);
      const cost = `${(u.time.cpu / 1e6).toFixed(2)} ms CPU, ${io.toFixed(2)} ms IO`;
      text = `W${w.id} processed split ${s.id}${att} of P${work.pipeline.index} (${s.label}): ${plural(u.ops[0]!.rowsOut, "row")} read, ${u.ops[u.ops.length - 1]!.rowsIn.toLocaleString("en")} into the sink; ${cost}${did.length ? `; ${did.join("; ")}` : ""}; SplitDone`;
    }
    this.record("process", text, end, refs, msgs, new Set([w.id]));
    if (error) this.error = error;
  }

  /** A failure: worker `w` stops just before the next thing happens. */
  stopWorker(w: number): void {
    if (this.done || this.coordinator.state.failed) throw new ExecutionError("the query is over");
    const node = this.workers[w];
    if (!node) throw new RangeError(`there is no worker W${w}`);
    if (node.gone) throw new RangeError(`W${w} is gone already`);
    const next = this.peekAction();
    const now = next ? next.at : this.events[this.events.length - 1]!.ms * 1e6;
    node.gone = true;
    // what it sent and was not delivered is lost with it
    const extra: LoggedMessage[] = [];
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const q = this.queue[i]!;
      if (q.event.type === "WorkerGone" || q.event.worker !== w) continue;
      this.queue.splice(i, 1);
      extra.unshift({ ...this.log(q.event, "lost", q.at), text: `lost with W${w}: ${describeMessage(q.event)}` });
    }
    const event: CoordinatorEvent = { type: "WorkerGone", worker: w };
    this.handling = { event, at: now, extra };
    this.coordinator.on(event, now);
    this.handling = null;
  }

  private check(): void {
    const f = this.coordinator.state.failed;
    if (!f) return;
    if (this.error && !this.thrown) {
      this.thrown = true;
      throw this.error;
    }
    throw new ExecutionError(`the query failed in ${f.unit}: ${f.error}`);
  }

  get done(): boolean {
    return this.coordinator.state.done;
  }

  get elapsedNs(): number {
    return Math.max(0, ...this.machines.map((m) => m.clock));
  }

  async step(): Promise<boolean> {
    this.check();
    if (this.done) return false;
    const before = this.events.length;
    while (this.events.length === before) {
      if (!(await this.advance())) throw internal("the query is stuck: nothing can happen");
    }
    this.check();
    return !this.done;
  }

  async wave(): Promise<number> {
    const used = new Set<number>();
    let ran = 0;
    for (;;) {
      this.check();
      if (this.done) break;
      const a = this.peekAction();
      if (!a) break;
      if (a.kind === "unit") {
        if (used.has(a.worker.id)) break;
        used.add(a.worker.id);
        ran++;
      }
      await this.advance();
    }
    this.check();
    return ran;
  }

  async runConcurrent(onProgress?: () => void): Promise<void> {
    const target = this.coordinator.state.pipelines.filter((p) => p.now.status === "running");
    while (!this.done && target.some((p) => p.now.status !== "done")) {
      await this.step();
      onProgress?.();
    }
  }

  async run(onProgress?: () => void): Promise<Row[]> {
    while (await this.step()) onProgress?.();
    return this.result();
  }

  result(): Row[] {
    if (!this.done) throw new ExecutionError("the query has not finished; step or run it");
    return this.coordinator.rows();
  }

  rows(): readonly Row[] {
    return this.coordinator.rows();
  }

  progress(): { done: number; total: number } {
    const s = this.coordinator.state.splits;
    return { done: s.filter((x) => x.now.status === "done" || x.now.status === "skipped").length, total: s.length };
  }

  // --- the workbench's view --------------------------------------------------------

  /** What happens after step `at`: the next split's step, or a peek at the simulation. */
  private nextAfter(at: number): Record<string, unknown> | null {
    for (let i = at + 1; i < this.events.length; i++) {
      const e = this.events[i]!;
      if (e.kind === "process") return { worker: e.worker, kind: "split", pipeline: e.pipeline, split: e.split, label: this.units.find((u) => u.seq === e.seq)?.label ?? "" };
    }
    const a = this.peekAction();
    if (!a) return null;
    if (a.kind === "deliver") return { worker: null, kind: "event", pipeline: null, split: null, label: describeMessage(this.queue[0]!.event) };
    const s = a.work.assigned.split;
    return { worker: a.worker.id, kind: "split", pipeline: a.work.pipeline.index, split: s.id, label: s.label };
  }

  /** Why a pending split was blocked right after step `at` (the coordinator's rule,
   * on the state as it was then). */
  private blockedAt(s: SplitState, at: number): string | null {
    const qs = this.coordinator.state;
    const x = s.split;
    const p = this.plan.pipelines[x.pipeline]!;
    if (x.kind === "buffer") {
      const b = qs.buffers[x.buffer]!;
      for (const o of x.outputs) {
        const st = b.outputs.get(outputKey(o))?.h.after(at)?.status;
        if (st !== "available") return `output (split ${o.split}, #${o.partition}) of B${x.buffer} is ${st ?? "missing"}`;
      }
    }
    for (const op of p.chain) {
      for (const wi of op.wholeInputs()) {
        const part = wi.per === "all" ? 0 : (x.partition ?? 0);
        if (qs.pipelines[wi.buffer.producer]!.h.after(at)?.status !== "done") return `${wi.buffer.name} is being made again`;
        for (const o of qs.buffers[wi.buffer.index]!.ofPartition(part)) {
          const st = o.h.after(at)?.status;
          if (st !== undefined && st !== "available") return `output (split ${o.split}, #${part}) of ${wi.buffer.name}, read whole, is ${st}`;
        }
      }
    }
    return null;
  }

  state(at = this.events.length - 1): Record<string, unknown> {
    at = Math.max(0, Math.min(at, this.events.length - 1));
    const qs = this.coordinator.state;
    const ev = this.events[at]!;
    const workers = this.frames[at]!;
    const done = this.events.slice(0, at + 1).some((e) => e.kind === "done");
    const failed = qs.failed && qs.failed.seq <= at ? { unit: qs.failed.unit, error: qs.failed.error } : null;
    const units = this.units.filter((u) => u.seq <= at);
    const ownersAt = (partitioning: number) => qs.owners.get(partitioning)?.after(at) ?? {};
    const splits = qs.splits.filter((s) => s.h.since <= at).map((s) => {
      const i = s.h.after(at)!;
      const x = s.split;
      // the worker ran it, and its SplitDone is on the way
      const ran = i.status === "queued" && units.some((u) => u.split === x.id && u.attempt === i.attempt && u.worker === i.worker && !u.failed);
      return {
        id: x.id, pipeline: x.pipeline, kind: x.kind, label: x.label, partition: x.partition,
        part: x.kind === "buffer" ? x.part : 0, parts: x.kind === "buffer" ? x.parts : 1, buffer: x.kind === "buffer" ? x.buffer : null,
        rows: x.rows, bytes: x.bytes, status: i.status, blocked: i.status === "pending" ? this.blockedAt(s, at) : null, ran,
        worker: i.worker, attempt: i.attempt, reason: i.reason,
        reads: x.kind === "buffer" ? x.outputs.map((o) => [o.split, o.partition]) : [],
      };
    });
    const pipelines = this.plan.pipelines.map((p) => {
      const ps = qs.pipelines[p.index]!;
      const i = ps.h.after(at)!;
      const ss = splits.filter((s) => s.pipeline === p.index);
      const count = (st: string) => ss.filter((s) => s.status === st).length;
      const r = p.source.reading();
      const out = p.sink.writes();
      const pu = units.filter((u) => u.pipeline === p.index && u.report);
      return {
        index: p.index, label: p.label(), depends_on: p.dependsOn, status: i.status, ready_ms: msOf(i.readyAt), start_ms: msOf(i.start), end_ms: msOf(i.end),
        completions: i.completions, reads: r.kind, whole: r.kind === "buffer" && r.whole, input: r.kind === "buffer" ? r.buffer.index : null,
        partitioning: r.kind === "buffer" ? r.buffer.partitioning : null, owners: r.kind === "buffer" ? ownersAt(r.buffer.partitioning) : {},
        writes: out?.index ?? null, builds: p.chain.flatMap((op) => op.wholeInputs().map((wi) => ({ buffer: wi.buffer.index, per: wi.per }))),
        root: p === this.plan.root,
        skipped_files: i.status === "waiting" ? [] : ps.skipped,
        splits: {
          total: ss.length, pending: count("pending"), blocked: ss.filter((s) => s.blocked).length, queued: count("queued"), done: count("done"),
          skipped: count("skipped"), failed: count("failed"), again: ss.filter((s) => s.attempt > 1).length,
        },
        ops: p.chain.map((op, j) => {
          const o = i.ops[j]!;
          const states = pu.slice(-50).map((u) => u.report!.ops[j]!.state);
          return {
            id: op.id, label: op.label(), state: toJson({ ...op.describe(), ...merge(states) }),
            metrics: {
              batches_in: o.batchesIn, rows_in: o.rowsIn, batches_out: o.batchesOut, rows_out: o.rowsOut, bytes_in: o.bytesIn, bytes_out: o.bytesOut,
              runs: o.runs, finished: i.status === "done", time_ms: Object.fromEntries(CATEGORIES.map((c) => [c, o.time[c] / 1e6])),
            },
          };
        }),
      };
    });
    const buffers = this.plan.buffers.map((b) => {
      const bs = qs.buffers[b.index]!;
      const outputs = [...bs.outputs.values()].filter((o) => o.h.since <= at).map((o) => {
        const i = o.h.after(at)!;
        return { split: o.split, partition: o.partition, worker: i.worker, rows: i.rows, bytes: i.bytes, pieces: i.pieces.length, status: i.status, attempt: i.attempt, new: o.h.since === at };
      }).sort((x, y) => x.partition - y.partition || x.split - y.split);
      return {
        index: b.index, name: b.name, mode: b.mode, label: b.label, partitions: b.n, partitioning: b.partitioning, producer: b.producer, consumers: b.consumers,
        status: bs.h.after(at) ?? "empty", enough: b.enough, owners: ownersAt(b.partitioning),
        rows: outputs.filter((o) => o.status === "available").reduce((a, o) => a + o.rows, 0),
        outputs,
      };
    });
    const elapsed = Math.max(ev.ms, ...workers.map((w) => (w as { clock_ms: number }).clock_ms));
    return {
      at, events: this.events.length, event: ev, done, failed,
      units_run: units.length, elapsed_ms: elapsed, next: done || failed ? null : this.nextAfter(at),
      cluster: this.cluster, settings: this.plan.settings, queue_depth: this.coordinator.settings.queueDepth,
      members: qs.cluster.workers.map((w) => ({ worker: w.id, status: w.h.after(at) ?? "up" })),
      pipelines, splits,
      units: units.map((u) => ({
        id: u.id, pipeline: u.pipeline, worker: u.worker, split: u.split, attempt: u.attempt, label: u.label, seq: u.seq, failed: u.failed,
        start_ms: u.start / 1e6, end_ms: u.end / 1e6,
        time_ms: Object.fromEntries(CATEGORIES.map((c) => [c, (u.report?.time[c] ?? 0) / 1e6])),
        segments: (u.report?.segments ?? []).map(([k, a, b]) => [k, a / 1e6, b / 1e6]),
        ops: (u.report?.ops ?? []).map((o) => ({ rows_in: o.rowsIn, rows_out: o.rowsOut, cpu_ms: o.time.cpu / 1e6, io_ms: (o.time.storage + o.time.network + o.time.spill_read + o.time.spill_write) / 1e6, notes: o.notes })),
      })),
      workers, buffers,
      filters: this.plan.filters.map((rf) => {
        const f = qs.filters[rf.id]!;
        const merged = f.merged.after(at) ?? null;
        return { id: rf.id, label: rf.label, producer: rf.producer, consumer: rf.consumer, keys: merged?.keys.length ?? null, range: merged && merged.lo !== null ? [merged.lo, merged.hi] : null };
      }),
      log: this.events.slice(Math.max(0, at - 199), at + 1).map((e) => [e.ms, e.text, e.kind, e.seq]),
      result_rows: qs.results.filter((r) => r.seq <= at).reduce((a, r) => a + r.rows.length, 0),
    };
  }
}

/** Instance states of one operator over its splits, as one: numbers summed, lists
 * joined (up to 20), anything else from the first split that has it. */
function merge(states: readonly Record<string, unknown>[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const s of states) {
    for (const [k, v] of Object.entries(s)) {
      const cur = out[k];
      if (typeof v === "number" && (typeof cur === "number" || cur === undefined)) out[k] = ((cur as number | undefined) ?? 0) + v;
      else if (Array.isArray(v) && v.every((x) => typeof x === "number") && Array.isArray(cur) && cur.length === v.length && cur.every((x) => typeof x === "number")) out[k] = cur.map((x: number, i: number) => x + (v[i] as number));
      else if (Array.isArray(v)) out[k] = [...((cur as unknown[] | undefined) ?? []), ...v].slice(0, 20);
      else if (typeof v === "boolean") out[k] = (cur as boolean | undefined) || v;
      else if (cur === undefined) out[k] = v;
    }
  }
  return out;
}
