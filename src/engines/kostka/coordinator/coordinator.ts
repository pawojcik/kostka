// The coordinator: the domain object that runs one query. It gets the physical plan
// and the workers, owns the QueryState, and acts only by sending commands; workers
// tell it what happened with events, and the infrastructure tells it when a worker is
// gone. It never reads rows, except the result.
//
//   pipeline ready  -> its splits (files, or cut from the input buffer's outputs);
//                      partition owners
//   dispatch        -> AddSplits: at most k queued per worker and pipeline; file
//                      splits anywhere, a partition's splits to its owner; a worker
//                      with nothing left gets a partition that has not started
//   SplitDone       -> its outputs (where its pieces are), keys, result rows; refill
//   all splits done -> the pipeline is done: its buffer is sealed, runtime filter
//                      keys merged, buffers nobody reads any more released, and
//                      pipelines waiting for it may become ready
//   WorkerGone      -> recovery from lineage: queued splits go back; the outputs it
//                      held are lost, and the splits that made the ones still needed
//                      run again (and so on, back to the scans); its partitions move
//
// Every decision is reported to the ports with what it means (`Decision`), so the
// history explains itself.

import type { BufferSplit, FileSplit, PieceRef, Split } from "../physical/buffer.ts";
import { outputKey } from "../physical/buffer.ts";
import { mergeKeys, misses, type FilterKeys } from "../physical/operators/runtime-filter.ts";
import { RuntimeFilterOp } from "../physical/operators/streaming.ts";
import { MAX_PLAN_ROWS, type LargestNode, type PhysicalPlan, type Pipeline } from "../physical/pipeline.ts";
import type { Assigned, Command, CoordinatorEvent, WholeInputPieces, WorkerEvent } from "../protocol.ts";
import type { Row } from "../../../core/types.ts";
import { addReport, History, OutputState, QueryState, SplitState, type PipelineState } from "./state.ts";

/** What a step of the history is about. */
export type StepKind =
  | "start" | "plan" | "assign" | "process" | "split_done" | "split_failed" | "pipeline_done" | "enough" | "worker_gone" | "done" | "failed";

export interface Refs {
  worker?: number;
  pipeline?: number;
  split?: number;
  buffer?: number;
  partition?: number;
}

/** A coordinator decision: what it means, and the commands it sends. */
export interface Decision {
  readonly kind: StepKind;
  readonly text: string;
  readonly commands: readonly Command[];
  readonly refs: Refs;
}

export interface CoordinatorPorts {
  /** Records a decision and sends its commands (the transport). */
  decide(d: Decision, now: number): void;
  /** The step changes made now belong to (for the history). */
  seq(): number;
}

export interface CoordinatorSettings {
  readonly workers: number;
  readonly splitRows: number;
  /** Splits queued per worker and pipeline at most. */
  readonly queueDepth: number;
}

const plural = (n: number, word: string) => `${n.toLocaleString("en")} ${word}${n === 1 ? "" : "s"}`;

/** A row count for people: 1,234,567 up to a trillion, 2.2 × 10^18 beyond. */
function count(rows: number): string {
  if (rows < 1e12) return Math.round(rows).toLocaleString("en");
  const e = Math.floor(Math.log10(rows));
  return `${(rows / 10 ** e).toFixed(1)} × 10^${e}`;
}

/** Roughly how long `rows` take at a browser's pace, a few million rows a second. */
function duration(rows: number): string {
  const s = rows / 4e6;
  const units: [number, string][] = [[365.25 * 86400, "year"], [86400, "day"], [3600, "hour"], [60, "minute"]];
  for (const [u, name] of units) if (s >= u) return `about ${count(Math.round(s / u))} ${name}${Math.round(s / u) === 1 ? "" : "s"}`;
  return `about ${Math.round(s)} seconds`;
}

/** Why the coordinator refuses a plan whose largest estimate is too many rows. */
function tooLarge(big: LargestNode): string {
  const why = big.cross
    ? " It is a cross join, with no join keys: if the query's join conditions are in its WHERE clause, the filter_pushdown rule turns them into join keys."
    : "";
  return `${big.node} is estimated to produce ${count(big.rows)} rows, more than the ${count(MAX_PLAN_ROWS)} this engine runs: at a few million rows a second it would take ${duration(big.rows)}.${why}`;
}
const list = (xs: readonly (string | number)[]) => xs.join(", ");
const W = (w: number) => `W${w}`;

export class Coordinator {
  readonly plan: PhysicalPlan;
  readonly settings: CoordinatorSettings;
  readonly state: QueryState;
  private readonly ports: CoordinatorPorts;
  /** "worker:pipeline" that got the pipeline's runtime filter keys. */
  private readonly primed = new Set<string>();

  constructor(plan: PhysicalPlan, settings: CoordinatorSettings, ports: CoordinatorPorts) {
    this.plan = plan;
    this.settings = settings;
    this.ports = ports;
    this.state = new QueryState(settings.workers, plan.pipelines.map((p) => ({ ops: p.chain.length })), plan.buffers.length, plan.filters.length);
  }

  private get seq(): number {
    return this.ports.seq();
  }

  private decide(now: number, kind: StepKind, text: string, commands: Command[] = [], refs: Refs = {}): void {
    this.ports.decide({ kind, text, commands, refs }, now);
  }

  private get over(): boolean {
    return this.state.failed !== null || this.state.done;
  }

  /** Submit: the query starts. */
  start(now = 0): void {
    const p = this.plan;
    this.decide(now, "start", `the query is submitted: ${plural(p.pipelines.length, "pipeline")}, ${plural(p.buffers.length, "output buffer")}, ${plural(this.settings.workers, "worker")}; every worker has the plan`);
    const big = p.largest;
    if (big && big.rows > MAX_PLAN_ROWS) {
      const error = tooLarge(big);
      this.state.failed = { unit: "the plan", error, name: "ExecutionError", seq: this.seq };
      this.decide(now, "failed", `the coordinator refuses the query: ${error}`);
      return;
    }
    this.schedule(now);
  }

  on(e: CoordinatorEvent, now: number): void {
    if (this.over) return;
    if (e.type === "WorkerGone") return this.workerGone(e.worker, now);
    if (!this.state.cluster.workers[e.worker]!.up) return; // it was gone before this arrived
    if (e.type === "SplitDone") return this.splitDone(e, now);
    return this.splitFailed(e, now);
  }

  // --- pipelines ------------------------------------------------------------------

  /** Pipelines whose inputs are all done start: their splits are made. */
  private schedule(now: number): void {
    for (const p of this.plan.pipelines) {
      const ps = this.state.pipelines[p.index]!;
      if (this.over || ps.now.status !== "waiting" || !p.dependsOn.every((i) => this.state.pipelines[i]!.now.status === "done")) continue;
      this.ready(p, ps, now);
    }
    this.dispatch(now);
  }

  private ready(p: Pipeline, ps: PipelineState, now: number): void {
    ps.update(this.seq, () => ({ status: "running", readyAt: now, start: now }));
    const splits = this.makeSplits(p, ps);
    for (const s of splits) {
      const st = new SplitState(s, this.seq);
      ps.splits.push(st);
      this.state.splits.push(st);
    }
    const r = p.source.reading();
    let owners = "";
    if (r.kind === "buffer" && splits.length) {
      const parts = [...new Set(splits.map((s) => s.partition!))].sort((a, b) => a - b);
      const o = this.assignOwners(r.buffer.partitioning, parts);
      const by = new Map<number, number[]>();
      for (const x of parts) by.set(o[x]!, [...(by.get(o[x]!) ?? []), x]);
      owners = `; partition owners (most bytes held first, balanced): ${[...by].sort((a, b) => a[0] - b[0]).map(([w, xs]) => `${W(w)} ${xs.map((x) => `#${x}`).join(" ")}`).join(", ")}`;
    }
    this.decide(now, "plan", this.describePlan(p, ps, splits) + owners, [], { pipeline: p.index, buffer: r.kind === "buffer" ? r.buffer.index : undefined });
    if (!splits.length) this.complete(p, ps, now);
  }

  private describePlan(p: Pipeline, ps: PipelineState, splits: Split[]): string {
    const r = p.source.reading();
    const skip = ps.skipped.length ? `; runtime filters ruled out ${plural(ps.skipped.length, "file")} (their min/max cannot hold a key)` : "";
    if (r.kind === "files") {
      if (!splits.length) return `P${p.index} is ready: no splits, there is nothing to read${skip}`;
      return `P${p.index} is ready: the coordinator makes ${plural(splits.length, "split")}, ${splits.length === 1 ? "a data file" : "a data file each"}${skip}`;
    }
    const b = this.state.buffers[r.buffer.index]!;
    if (!splits.length) return `P${p.index} is ready: ${r.buffer.name} is empty, so there are no splits`;
    const parts = new Set(splits.map((s) => s.partition)).size;
    return `P${p.index} is ready: ${r.buffer.name} has ${plural(b.outputs.size, "output")} (what each split of P${r.buffer.producer} wrote to each partition); the coordinator ${r.whole ? "puts each partition in one split (it is read whole)" : `cuts them into splits of up to ${this.settings.splitRows.toLocaleString("en")} rows`}: ${plural(splits.length, "split")} over ${plural(parts, "partition")}`;
  }

  /** Files minus those runtime filters rule out; or a buffer's outputs, per
   * partition, joined up to splitRows (whole partitions for a reader that needs them). */
  private makeSplits(p: Pipeline, ps: PipelineState): Split[] {
    const r = p.source.reading();
    let id = this.state.splits.length;
    if (r.kind === "files") {
      const out: FileSplit[] = [];
      for (const file of r.files) {
        const ruled = r.filters.some((rf) => {
          const keys = this.state.filters[rf.id]!.merged.now;
          return keys !== null && misses(keys, file.stats.get(rf.column));
        });
        if (ruled) {
          ps.skipped.push(file.path);
          continue;
        }
        out.push({ kind: "file", id: id++, pipeline: p.index, file, partition: null, rows: file.rowCount, bytes: r.bytes(file), label: file.path });
      }
      return out;
    }
    const b = this.state.buffers[r.buffer.index]!;
    const out: BufferSplit[] = [];
    for (let part = 0; part < r.buffer.n; part++) {
      const outputs = b.ofPartition(part).filter((o) => o.now.rows > 0)
        .sort((x, y) => x.now.worker - y.now.worker || x.split - y.split);
      if (!outputs.length && !r.always.includes(part)) continue;
      const groups: OutputState[][] = [];
      if (r.whole) groups.push(outputs);
      else {
        let cur: OutputState[] = [], rows = 0;
        for (const o of outputs) {
          if (cur.length && rows + o.now.rows > this.settings.splitRows) {
            groups.push(cur);
            cur = [];
            rows = 0;
          }
          cur.push(o);
          rows += o.now.rows;
        }
        if (cur.length) groups.push(cur);
      }
      groups.forEach((g, i) => {
        const label = (r.buffer.n > 1 ? `partition ${part}` : r.whole ? "all of it" : "rows") + (groups.length > 1 ? ` (${i + 1}/${groups.length})` : "");
        out.push({
          kind: "buffer", id: id++, pipeline: p.index, buffer: r.buffer.index, partition: part,
          outputs: g.map((o) => ({ split: o.split, partition: o.partition })), part: i, parts: groups.length,
          rows: g.reduce((a, o) => a + o.now.rows, 0), bytes: g.reduce((a, o) => a + o.now.bytes, 0), label,
        });
      });
    }
    return out;
  }

  /** Gives the partitions of a partitioning that have no (live) owner one: the up
   * worker holding most of the partition's bytes, keeping the counts balanced. */
  private assignOwners(partitioning: number, parts: number[]): Record<number, number> {
    const up = this.state.cluster.up;
    const known = { ...this.state.ownersOf(partitioning) };
    const todo = parts.filter((p) => known[p] === undefined || !up.includes(known[p]!));
    if (!todo.length) return known;
    const held = new Map<string, number>();
    for (const buf of this.plan.buffers) {
      if (buf.partitioning !== partitioning) continue;
      for (const o of this.state.buffers[buf.index]!.outputs.values()) {
        if (!o.available) continue;
        const k = `${o.now.worker}:${o.partition}`;
        held.set(k, (held.get(k) ?? 0) + o.now.bytes);
      }
    }
    const h = (w: number, p: number) => held.get(`${w}:${p}`) ?? 0;
    const total = (p: number) => up.reduce((a, w) => a + h(w, p), 0);
    const all = [...new Set([...parts, ...Object.keys(known).map(Number)])];
    const cap = Math.ceil(all.length / up.length);
    const count = new Map(up.map((w) => [w, 0])), load = new Map(up.map((w) => [w, 0]));
    for (const [p, w] of Object.entries(known)) {
      if (!count.has(w)) continue;
      count.set(w, count.get(w)! + 1);
      load.set(w, load.get(w)! + total(Number(p)));
    }
    for (const p of [...todo].sort((a, b) => total(b) - total(a) || a - b)) {
      let best = -1;
      for (const w of up) {
        if (count.get(w)! >= cap) continue;
        const better = best < 0 || h(w, p) > h(best, p) || (h(w, p) === h(best, p) && load.get(w)! < load.get(best)!);
        if (better) best = w;
      }
      if (best < 0) best = [...up].sort((a, b) => count.get(a)! - count.get(b)! || a - b)[0]!;
      known[p] = best;
      count.set(best, count.get(best)! + 1);
      load.set(best, load.get(best)! + total(p));
    }
    this.setOwners(partitioning, known);
    return known;
  }

  private setOwners(partitioning: number, owners: Record<number, number>): void {
    const h = this.state.owners.get(partitioning);
    if (h) h.set(this.seq, owners);
    else this.state.owners.set(partitioning, new History(this.seq, owners));
  }

  // --- splits: what may run, and where --------------------------------------------

  /** Why a pending split cannot run yet, or null: an output it reads is not
   * available (lost or released, and being made again). */
  blocked(s: SplitState): string | null {
    const x = s.split;
    const p = this.plan.pipelines[x.pipeline]!;
    if (x.kind === "buffer") {
      const b = this.state.buffers[x.buffer]!;
      for (const o of x.outputs) {
        const out = b.outputs.get(outputKey(o));
        if (!out?.available) return `output (split ${o.split}, #${o.partition}) of B${x.buffer} is ${out?.now.status ?? "missing"}`;
      }
    }
    for (const op of p.chain) {
      for (const wi of op.wholeInputs()) {
        const part = wi.per === "all" ? 0 : (x.partition ?? 0);
        const producer = this.state.pipelines[wi.buffer.producer]!;
        if (producer.now.status !== "done") return `${wi.buffer.name} is being made again`;
        const lost = this.state.buffers[wi.buffer.index]!.ofPartition(part).find((o) => !o.available);
        if (lost) return `output (split ${lost.split}, #${part}) of ${wi.buffer.name}, read whole, is ${lost.now.status}`;
      }
    }
    return null;
  }

  private owner(p: Pipeline, partition: number): number | undefined {
    const r = p.source.reading();
    return r.kind === "buffer" ? this.state.ownersOf(r.buffer.partitioning)[partition] : undefined;
  }

  private queuedOn(ps: PipelineState, w: number): SplitState[] {
    return ps.splits.filter((s) => s.now.status === "queued" && s.now.worker === w);
  }

  /** Pending splits of `p` that worker `w` may get now, in the order it should. */
  private eligible(p: Pipeline, ps: PipelineState, w: number): SplitState[] {
    const files = p.source.reading().kind === "files";
    return ps.splits.filter((s) => s.now.status === "pending" && (files || this.owner(p, s.split.partition!) === w) && !this.blocked(s));
  }

  /** Gives every up worker splits of every running pipeline, while it has room. */
  private dispatch(now: number): void {
    for (const p of this.plan.pipelines) {
      const ps = this.state.pipelines[p.index]!;
      if (this.over || ps.now.status !== "running") continue;
      const up = this.state.cluster.up;
      // a fair share of the files at first, so that every worker gets some
      const files = p.source.reading().kind === "files";
      const share = files ? Math.ceil(ps.splits.filter((s) => s.now.status === "pending" && !this.blocked(s)).length / Math.max(1, up.length)) : Infinity;
      // file splits go to the least loaded workers first, so that independent
      // pipelines with a split or two each don't all land on the first worker
      const order = files ? this.byLoad(up) : up;
      for (const w of order) {
        const r = this.fill(p, ps, w, share);
        if (r) this.decide(now, "assign", r.text, r.commands, { worker: w, pipeline: p.index, split: r.first, partition: r.partition });
      }
    }
  }

  /** Up workers, least loaded first: by the rows of splits queued on them (of every
   * pipeline), then by how many, then by number. */
  private byLoad(up: readonly number[]): number[] {
    const rows = new Map(up.map((w) => [w, 0])), count = new Map(up.map((w) => [w, 0]));
    for (const ps of this.state.pipelines) {
      for (const s of ps.splits) {
        if (s.now.status !== "queued" || !rows.has(s.now.worker!)) continue;
        rows.set(s.now.worker!, rows.get(s.now.worker!)! + s.split.rows);
        count.set(s.now.worker!, count.get(s.now.worker!)! + 1);
      }
    }
    return [...up].sort((a, b) => rows.get(a)! - rows.get(b)! || count.get(a)! - count.get(b)! || a - b);
  }

  /** Fills worker `w`'s queue for `p` up to k. With nothing left for it, a partition
   * none of whose splits went out yet moves to it from its owner. */
  private fill(p: Pipeline, ps: PipelineState, w: number, share = Infinity): { text: string; commands: Command[]; first: number; partition?: number } | null {
    if (ps.now.status !== "running" || !this.state.cluster.workers[w]!.up) return null;
    const queued = this.queuedOn(ps, w).length;
    const room = this.settings.queueDepth - queued;
    if (room <= 0) return null;
    const texts: string[] = [];
    let mine = this.eligible(p, ps, w);
    let moved: number | null = null;
    const r = p.source.reading();
    if (!mine.length && !queued && r.kind === "buffer") {
      moved = this.move(p, ps, w);
      if (moved !== null) {
        texts.push(`${W(w)} has nothing to do and partition #${moved} has not started on its owner: it moves to ${W(w)}`);
        mine = this.eligible(p, ps, w);
      }
    }
    const pick = mine.slice(0, Math.max(0, Math.min(room, share)));
    if (!pick.length) return null;
    const assigned: Assigned[] = [];
    for (const s of pick) {
      const attempt = s.now.attempt + 1;
      const reason = r.kind === "files" ? (s.now.attempt ? `runs again on ${W(w)}` : `a file split, to the least loaded worker with room`)
        : moved === s.split.partition ? `partition #${moved} moved to ${W(w)}` : `${W(w)} owns partition #${s.split.partition}`;
      s.set(this.seq, { status: "queued", worker: w, attempt, reason });
      assigned.push({ split: s.split, attempt, pieces: this.piecesOf(s.split), inputs: this.inputsOf(p, s.split) });
    }
    const key = `${w}:${p.index}`;
    let filters: FilterKeys[] = [];
    if (!this.primed.has(key)) {
      this.primed.add(key);
      const ids = new Set(p.operators.filter((op) => op instanceof RuntimeFilterOp).flatMap((op) => (op as RuntimeFilterOp).filters.map(([, rf]) => rf.id)));
      filters = [...ids].map((id) => this.state.filters[id]!.merged.now).filter((f): f is FilterKeys => f !== null);
    }
    texts.push(`AddSplits ${list(pick.map((s) => `${s.split.id} (${s.split.label}, ${s.split.rows.toLocaleString("en")} rows${s.now.attempt > 1 ? `, attempt ${s.now.attempt}` : ""})`))} of P${p.index} to ${W(w)}: ${pick[0]!.now.reason}`);
    const builds = assigned.flatMap((a) => a.inputs.map((i) => `${this.plan.buffers[i.buffer]!.name}${this.plan.buffers[i.buffer]!.n > 1 ? ` #${i.partition}` : ""}`));
    if (builds.length) texts.push(`with the locations of ${list([...new Set(builds)])} (read whole)`);
    if (filters.length) texts.push(`and the keys of ${plural(filters.length, "runtime filter")}`);
    texts.push(`${plural(this.queuedOn(ps, w).length, "split")} of P${p.index} queued on ${W(w)}; ${plural(ps.splits.filter((s) => s.now.status === "pending").length, "split")} not given out yet`);
    return { text: texts.join("; "), commands: [{ type: "AddSplits", worker: w, pipeline: p.index, splits: assigned, filters }], first: pick[0]!.split.id, partition: pick[0]!.split.partition ?? undefined };
  }

  /** Moves a partition of `p` none of whose splits went out yet (no stealing of
   * started partitions) from its owner to idle worker `w`. */
  private move(p: Pipeline, ps: PipelineState, w: number): number | null {
    const r = p.source.reading();
    if (r.kind !== "buffer") return null;
    const owners = this.state.ownersOf(r.buffer.partitioning);
    // the owner with most partitions waiting gives up its last
    const waiting = new Map<number, number[]>();
    for (const [x, o] of Object.entries(owners)) {
      const part = Number(x);
      if (o === w) continue;
      const splits = ps.splits.filter((s) => s.split.partition === part);
      if (!splits.length || !splits.every((s) => s.now.status === "pending" && s.now.attempt === 0 && !this.blocked(s))) continue;
      // another pipeline of the partitioning may have started it (a hash table built there)
      waiting.set(o, [...(waiting.get(o) ?? []), part]);
    }
    const from = [...waiting].sort((a, b) => b[1].length - a[1].length || a[0] - b[0])[0];
    if (!from) return null;
    const part = from[1][from[1].length - 1]!;
    this.setOwners(r.buffer.partitioning, { ...owners, [part]: w });
    return part;
  }

  /** The pieces of the outputs a buffer split reads, where they are now. */
  private piecesOf(s: Split): PieceRef[] {
    if (s.kind !== "buffer") return [];
    const b = this.state.buffers[s.buffer]!;
    return s.outputs.flatMap((o) => b.outputs.get(outputKey(o))!.now.pieces);
  }

  /** The buffers a split of `p` reads whole (build sides): the pieces of its partition. */
  private inputsOf(p: Pipeline, s: Split): WholeInputPieces[] {
    const out: WholeInputPieces[] = [];
    for (const op of p.chain) {
      for (const wi of op.wholeInputs()) {
        const partition = wi.per === "all" ? 0 : (s.partition ?? 0);
        const b = this.state.buffers[wi.buffer.index]!;
        out.push({ buffer: wi.buffer.index, partition, pieces: b.ofPartition(partition).flatMap((o) => o.now.pieces) });
      }
    }
    return out;
  }

  // --- events ---------------------------------------------------------------------

  private splitDone(e: Extract<WorkerEvent, { type: "SplitDone" }>, now: number): void {
    const s = this.state.split(e.split);
    if (s.now.status !== "queued" || s.now.worker !== e.worker || s.now.attempt !== e.attempt) return;
    const p = this.plan.pipelines[e.pipeline]!;
    const ps = this.state.pipelines[p.index]!;
    s.set(this.seq, { status: "done", reason: `done on ${W(e.worker)}` });
    ps.update(this.seq, (x) => ({ ops: x.ops.map((o, i) => addReport(o, e.unit.ops[i]!)) }));
    const texts: string[] = [];
    // its outputs: the pieces it wrote, by partition
    const out = p.sink.writes();
    if (out) {
      const b = this.state.buffers[out.index]!;
      const by = new Map<number, PieceRef[]>();
      for (const piece of e.pieces) by.set(piece.partition, [...(by.get(piece.partition) ?? []), piece]);
      for (const [part, pieces] of [...by].sort((a, c) => a[0] - c[0])) {
        const info = { status: "available" as const, worker: e.worker, pieces, rows: pieces.reduce((a, x) => a + x.rows, 0), bytes: pieces.reduce((a, x) => a + x.bytes, 0), attempt: e.attempt };
        const k = outputKey({ split: e.split, partition: part });
        const o = b.outputs.get(k);
        if (o) o.h.set(this.seq, info);
        else b.outputs.set(k, new OutputState(out.index, e.split, part, info, this.seq));
      }
      if (b.h.now === "empty") b.h.set(this.seq, "filling");
      const rows = e.pieces.reduce((a, x) => a + x.rows, 0);
      texts.push(by.size ? `its ${plural(by.size, "output")} of ${out.name} (${plural(rows, "row")}${out.n > 1 ? ` in ${[...by.keys()].sort((a, c) => a - c).map((x) => `#${x}`).join(" ")}` : ""}) are on ${W(e.worker)}` : `it wrote nothing to ${out.name}`);
    }
    for (const f of e.filters) this.state.filters[f.filter]!.parts.set(e.split, f);
    if (e.filters.length) texts.push(`${plural(e.filters.reduce((a, f) => a + f.keys.length, 0), "runtime filter key")} kept for later`);
    if (e.result) {
      this.state.results.push({ split: e.split, rows: e.result, seq: this.seq });
      texts.push(`${plural(e.result.length, "result row")}`);
    }
    const left = ps.splits.filter((x) => x.now.status === "pending" || x.now.status === "queued").length;
    const refill = this.fill(p, ps, e.worker);
    if (refill) texts.push(refill.text);
    else if (left) texts.push(`${plural(left, "split")} of P${p.index} not done yet`);
    this.decide(now, "split_done", `${W(e.worker)} reported split ${e.split}${e.attempt > 1 ? ` (attempt ${e.attempt})` : ""} of P${p.index} done: ${texts.join("; ")}`, refill?.commands ?? [], { worker: e.worker, pipeline: p.index, split: e.split });
    if (out?.enough !== null && out?.enough !== undefined) this.enough(p, ps, now);
    if (ps.splits.every((x) => x.now.status === "done" || x.now.status === "skipped") && ps.now.status === "running") this.complete(p, ps, now);
    else this.dispatch(now);
  }

  /** A gather for a Limit holds enough rows: the producer's pending splits are skipped. */
  private enough(p: Pipeline, ps: PipelineState, now: number): void {
    const out = p.sink.writes()!;
    const b = this.state.buffers[out.index]!;
    const rows = [...b.outputs.values()].filter((o) => o.available).reduce((a, o) => a + o.now.rows, 0);
    const pending = ps.splits.filter((s) => s.now.status === "pending");
    if (rows < out.enough! || !pending.length) return;
    for (const s of pending) s.set(this.seq, { status: "skipped", reason: `${out.name} already holds ${rows.toLocaleString("en")} rows, enough for the Limit` });
    this.decide(now, "enough", `${out.name} holds ${plural(rows, "row")}, enough for the Limit of ${out.enough}: the coordinator skips P${p.index}'s ${plural(pending.length, "pending split")} (the queued ones still run)`, [], { pipeline: p.index, buffer: out.index });
  }

  private splitFailed(e: Extract<WorkerEvent, { type: "SplitFailed" }>, now: number): void {
    const s = this.state.split(e.split);
    if (s.now.status !== "queued" || s.now.worker !== e.worker || s.now.attempt !== e.attempt) return;
    const refs = { worker: e.worker, pipeline: e.pipeline, split: e.split };
    if (e.lost) {
      s.set(this.seq, { status: "pending", worker: null, reason: `a piece it read was on ${W(e.lost.worker)}, which is gone: it runs again once that output is made again` });
      this.decide(now, "split_failed", `split ${e.split} of P${e.pipeline} failed on ${W(e.worker)}: a piece it read was on ${W(e.lost.worker)}, which is gone. It goes back to pending (blocked: ${this.blocked(s) ?? "not any more"})`, [], refs);
      this.dispatch(now);
      return;
    }
    s.set(this.seq, { status: "failed", reason: e.error });
    const unit = `split ${e.split} of P${e.pipeline} on ${W(e.worker)}`;
    this.state.failed = { unit, error: e.error, name: e.name, seq: this.seq };
    this.decide(now, "failed", `the query failed in ${unit}: ${e.error}`, [], refs);
  }

  // --- recovery -------------------------------------------------------------------

  private workerGone(w: number, now: number): void {
    const ws = this.state.cluster.workers[w]!;
    if (!ws.up) return;
    ws.h.set(this.seq, "gone");
    const texts: string[] = [];
    // 1. its queued splits go back
    const requeued: number[] = [];
    for (const s of this.state.splits) {
      if (s.now.status !== "queued" || s.now.worker !== w) continue;
      s.set(this.seq, { status: "pending", worker: null, reason: `${W(w)} is gone before it reported this split` });
      requeued.push(s.split.id);
    }
    if (requeued.length) texts.push(`${plural(requeued.length, "split")} queued on it go${requeued.length === 1 ? "es" : ""} back to pending (${list(requeued)})`);
    // 2. the outputs it held are lost
    const lost: string[] = [];
    for (const b of this.state.buffers) {
      let n = 0;
      for (const o of b.outputs.values()) {
        if (o.now.worker !== w || o.now.status !== "available") continue;
        o.h.set(this.seq, { ...o.now, status: "lost", pieces: [] });
        n++;
      }
      if (n) lost.push(`${n} of ${this.plan.buffers[b.index]!.name}`);
    }
    if (lost.length) texts.push(`its outputs are lost: ${list(lost)}`);
    // 3. its partitions move
    const up = this.state.cluster.up;
    const moved: string[] = [];
    for (const [partitioning, h] of this.state.owners) {
      const owners = { ...h.now };
      const mine = Object.entries(owners).filter(([, o]) => o === w).map(([x]) => Number(x));
      if (!mine.length || !up.length) continue;
      const count = new Map(up.map((x) => [x, Object.values(owners).filter((o) => o === x).length]));
      for (const x of mine) {
        const to = [...up].sort((a, b) => count.get(a)! - count.get(b)! || a - b)[0]!;
        owners[x] = to;
        count.set(to, count.get(to)! + 1);
        moved.push(`#${x} → ${W(to)}`);
      }
      this.setOwners(partitioning, owners);
    }
    if (moved.length) texts.push(`its partitions move: ${list(moved)}`);
    if (!up.length) {
      this.state.failed = { unit: `W${w}`, error: "every worker is gone", name: "ExecutionError", seq: this.seq };
      this.decide(now, "failed", `${W(w)} is gone, and it was the last worker: the query fails`, [], { worker: w });
      return;
    }
    // 4. the splits that made outputs still needed run again
    const again = this.recover();
    if (again.length) {
      const by = new Map<number, number[]>();
      for (const s of again) by.set(s.split.pipeline, [...(by.get(s.split.pipeline) ?? []), s.split.id]);
      texts.push(`lineage: ${[...by].map(([p, ids]) => `${plural(ids.length, "split")} of P${p} (${list(ids.slice(0, 12))}${ids.length > 12 ? ", …" : ""})`).join(", ")} run${again.length === 1 ? "s" : ""} again, to make outputs still needed`);
    } else if (lost.length) texts.push("no lost output is still needed");
    this.decide(now, "worker_gone", `${W(w)} is gone. ${texts.join("; ") || "It held nothing the query needs"}`, [], { worker: w });
    // pipelines running again may make others wait; anything left runs elsewhere
    const allDone = this.plan.pipelines.every((p) => this.state.pipelines[p.index]!.now.status === "done");
    if (!allDone) this.dispatch(now);
  }

  /** Who still needs output `o`: a split not done that reads it, a buffer read
   * whole by a split not done, or a reader that has not started. */
  private needed(o: OutputState): boolean {
    const buf = this.plan.buffers[o.buffer]!;
    for (const c of buf.consumers) {
      const cp = this.plan.pipelines[c]!;
      const cs = this.state.pipelines[c]!;
      if (cs.now.status === "waiting") return true;
      const live = cs.splits.filter((s) => s.now.status === "pending" || s.now.status === "queued");
      const r = cp.source.reading();
      if (r.kind === "buffer" && r.buffer.index === o.buffer && live.some((s) => s.split.kind === "buffer" && s.split.outputs.some((x) => x.split === o.split && x.partition === o.partition))) return true;
      for (const op of cp.chain) {
        for (const wi of op.wholeInputs()) {
          if (wi.buffer.index !== o.buffer) continue;
          if (live.some((s) => wi.per === "all" || (s.split.partition ?? 0) === o.partition)) return true;
        }
      }
    }
    return false;
  }

  /** Runs again, from lineage, every done split whose output is lost or released
   * but still needed; to a fixpoint, since a split run again needs its own inputs. */
  private recover(): SplitState[] {
    const again: SplitState[] = [];
    for (let changed = true; changed;) {
      changed = false;
      for (const b of this.state.buffers) {
        const buf = this.plan.buffers[b.index]!;
        for (const o of b.outputs.values()) {
          if (o.available) continue;
          const s = this.state.split(o.split);
          if (s.now.status !== "done" || !this.needed(o)) continue;
          const readers = buf.consumers.map((c) => `P${c}`).join(", ");
          s.set(this.seq, { status: "pending", worker: null, reason: `runs again: its output #${o.partition} of ${buf.name} was ${o.now.status === "lost" ? `lost with W${o.now.worker}` : "released"}, and ${readers} still need${buf.consumers.length === 1 ? "s" : ""} it` });
          again.push(s);
          changed = true;
          const ps = this.state.pipelines[s.split.pipeline]!;
          if (ps.now.status === "done") ps.update(this.seq, () => ({ status: "running", end: null }));
          if (b.h.now === "sealed" || b.h.now === "released") b.h.set(this.seq, "filling");
        }
      }
    }
    return again;
  }

  // --- pipelines done -------------------------------------------------------------

  private complete(p: Pipeline, ps: PipelineState, now: number): void {
    const again = ps.now.completions > 0;
    ps.update(this.seq, (x) => ({ status: "done", end: now, completions: x.completions + 1 }));
    const texts: string[] = [`P${p.index} is done${again ? " again (it remade lost outputs)" : ""}`];
    const out = p.sink.writes();
    if (out) {
      const b = this.state.buffers[out.index]!;
      b.h.set(this.seq, "sealed");
      const live = [...b.outputs.values()].filter((o) => o.available);
      const rows = live.reduce((a, o) => a + o.now.rows, 0);
      const holders = [...new Set(live.map((o) => o.now.worker))].sort((a, c) => a - c);
      const sizes = Array.from({ length: out.n }, (_, i) => b.ofPartition(i).filter((o) => o.available).reduce((a, o) => a + o.now.rows, 0));
      texts.push(`${out.name} (${out.describe()}) is sealed: ${plural(rows, "row")} in ${plural(live.length, "output")}${holders.length ? ` on ${list(holders.map(W))}` : ""}${out.n > 1 ? `, partition rows ${sizes.map((x) => x.toLocaleString("en")).join(" / ")}` : ""}`);
    }
    for (const rf of this.plan.filters) {
      if (rf.producer !== p.index) continue;
      const f = this.state.filters[rf.id]!;
      const merged = mergeKeys(rf.id, [...f.parts.values()]);
      f.merged.set(this.seq, merged);
      texts.push(`runtime filter ${rf.label}: ${plural(merged.keys.length, "key")} merged from ${plural(f.parts.size, "split")}`);
    }
    const commands = this.release(texts);
    const waiting = this.plan.pipelines.filter((x) => this.state.pipelines[x.index]!.now.status === "waiting" && x.dependsOn.includes(p.index)).map((x) => `P${x.index}`);
    if (waiting.length) texts.push(`${list(waiting)} no longer ${waiting.length === 1 ? "waits" : "wait"} for it`);
    this.decide(now, "pipeline_done", texts.join("; "), commands, { pipeline: p.index, buffer: out?.index });
    if (this.state.pipelines.every((x) => x.now.status === "done")) {
      this.state.done = true;
      this.decide(now, "done", `the query is done: ${plural(this.state.results.reduce((a, r) => a + r.rows.length, 0), "result row")}`);
      return;
    }
    this.schedule(now);
  }

  /** Releases the buffers every reader is done with, on every worker up. */
  private release(texts: string[]): Command[] {
    const commands: Command[] = [];
    const released: string[] = [];
    for (const buf of this.plan.buffers) {
      const b = this.state.buffers[buf.index]!;
      if (b.h.now !== "sealed" || !buf.consumers.length || !buf.consumers.every((i) => this.state.pipelines[i]!.now.status === "done")) continue;
      b.h.set(this.seq, "released");
      for (const o of b.outputs.values()) if (o.available) o.h.set(this.seq, { ...o.now, status: "released", pieces: [] });
      for (const w of this.state.cluster.up) commands.push({ type: "ReleaseBuffer", worker: w, buffer: buf.index });
      released.push(buf.name);
    }
    if (released.length) texts.push(`every reader of ${list(released)} is done: ReleaseBuffer on every worker`);
    return commands;
  }

  /** The result rows so far, in the order their splits reported them. */
  rows(): Row[] {
    return this.state.results.flatMap((r) => r.rows as Row[]);
  }
}
