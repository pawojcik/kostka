// What the coordinator knows: the ClusterState (which workers are up) and the
// QueryState of its query. Both are built only from the plan and from events:
// which splits exist, where they went and how often they ran; every output (what a
// split wrote to a partition of a buffer: where its pieces are, never their rows)
// and whether it is still there; pipelines, partition owners, runtime filter keys,
// operator metrics and the result.
//
// Each change remembers the step it happened at (`seq`), so the workbench can show
// the state as it was after any step.

import type { Row } from "../../../core/types.ts";
import { History } from "../../../core/history.ts";
import type { PieceRef, Split } from "../physical/buffer.ts";
import { CATEGORIES, type Category } from "../physical/context.ts";
import type { FilterKeys } from "../physical/operators/runtime-filter.ts";
import type { OpReport } from "../protocol.ts";

export type PipelineStatus = "waiting" | "running" | "done";
export type SplitStatus = "pending" | "queued" | "done" | "skipped" | "failed";
export type OutputStatus = "available" | "lost" | "released";
export type BufferStatus = "empty" | "filling" | "sealed" | "released";
export type WorkerStatus = "up" | "gone";

export class WorkerState {
  readonly id: number;
  readonly h: History<WorkerStatus>;
  constructor(id: number) {
    this.id = id;
    this.h = new History<WorkerStatus>(0, "up");
  }
  get up(): boolean {
    return this.h.now === "up";
  }
}

/** The cluster as the coordinator knows it. No new workers are provisioned: a gone
 * worker stays gone. */
export class ClusterState {
  readonly workers: WorkerState[];
  constructor(n: number) {
    this.workers = Array.from({ length: n }, (_, i) => new WorkerState(i));
  }
  get up(): number[] {
    return this.workers.filter((w) => w.up).map((w) => w.id);
  }
}

export interface SplitInfo {
  status: SplitStatus;
  worker: number | null;
  /** How many times it was given out to run (1 the first time). */
  attempt: number;
  /** Why it has its status (or went to its worker). */
  reason: string | null;
}

export class SplitState {
  readonly split: Split;
  readonly h: History<SplitInfo>;
  constructor(split: Split, seq: number) {
    this.split = split;
    this.h = new History(seq, { status: "pending", worker: null, attempt: 0, reason: null });
  }
  get now(): SplitInfo {
    return this.h.now;
  }
  set(seq: number, f: Partial<SplitInfo>): void {
    this.h.set(seq, { ...this.now, ...f });
  }
}

export interface OutputInfo {
  status: OutputStatus;
  worker: number;
  pieces: readonly PieceRef[];
  rows: number;
  bytes: number;
  /** The attempt of its split that made it. */
  attempt: number;
}

/** What split `split` wrote to `partition` of a buffer. */
export class OutputState {
  readonly buffer: number;
  readonly split: number;
  readonly partition: number;
  readonly h: History<OutputInfo>;
  constructor(buffer: number, split: number, partition: number, info: OutputInfo, seq: number) {
    this.buffer = buffer;
    this.split = split;
    this.partition = partition;
    this.h = new History(seq, info);
  }
  get now(): OutputInfo {
    return this.h.now;
  }
  get available(): boolean {
    return this.now.status === "available";
  }
}

/** An operator's totals over every split so far. */
export interface OpTotals {
  rowsIn: number;
  rowsOut: number;
  batchesIn: number;
  batchesOut: number;
  bytesIn: number;
  bytesOut: number;
  runs: number;
  time: Record<Category, number>;
}

export const emptyTotals = (): OpTotals => ({
  rowsIn: 0, rowsOut: 0, batchesIn: 0, batchesOut: 0, bytesIn: 0, bytesOut: 0, runs: 0,
  time: Object.fromEntries(CATEGORIES.map((c) => [c, 0])) as Record<Category, number>,
});

export function addReport(t: OpTotals, r: OpReport): OpTotals {
  const time = { ...t.time };
  for (const c of CATEGORIES) time[c] += r.time[c];
  return {
    rowsIn: t.rowsIn + r.rowsIn, rowsOut: t.rowsOut + r.rowsOut, batchesIn: t.batchesIn + r.batchesIn, batchesOut: t.batchesOut + r.batchesOut,
    bytesIn: t.bytesIn + r.bytesIn, bytesOut: t.bytesOut + r.bytesOut, runs: t.runs + 1, time,
  };
}

export interface PipelineInfo {
  status: PipelineStatus;
  readyAt: number | null;
  start: number | null;
  end: number | null;
  /** How many times it was done (more than once after a recovery). */
  completions: number;
  ops: OpTotals[];
}

export class PipelineState {
  readonly index: number;
  readonly h: History<PipelineInfo>;
  readonly splits: SplitState[] = [];
  /** Scans: files a runtime filter ruled out. */
  skipped: string[] = [];
  constructor(index: number, ops: number, seq: number) {
    this.index = index;
    this.h = new History(seq, { status: "waiting", readyAt: null, start: null, end: null, completions: 0, ops: Array.from({ length: ops }, emptyTotals) });
  }
  get now(): PipelineInfo {
    return this.h.now;
  }
  update(seq: number, f: (p: PipelineInfo) => Partial<PipelineInfo>): void {
    const p = this.now;
    this.h.set(seq, { ...p, ...f(p) });
  }
}

export class BufferState {
  readonly index: number;
  readonly h: History<BufferStatus>;
  /** "split:partition" -> its output. */
  readonly outputs = new Map<string, OutputState>();
  constructor(index: number, seq: number) {
    this.index = index;
    this.h = new History<BufferStatus>(seq, "empty");
  }
  ofPartition(partition: number): OutputState[] {
    return [...this.outputs.values()].filter((o) => o.partition === partition);
  }
}

export interface FilterState {
  /** Keys reported by the producer's splits (by split: a split run again reports again). */
  readonly parts: Map<number, FilterKeys>;
  readonly merged: History<FilterKeys | null>;
}

export class QueryState {
  readonly cluster: ClusterState;
  readonly pipelines: PipelineState[];
  readonly buffers: BufferState[];
  readonly splits: SplitState[] = [];
  /** Partitioning -> partition -> owner worker. */
  readonly owners = new Map<number, History<Record<number, number>>>();
  readonly filters: FilterState[];
  /** The result: rows by split, in the order they arrived. */
  readonly results: { split: number; rows: readonly Row[]; seq: number }[] = [];
  failed: { unit: string; error: string; name: string; seq: number } | null = null;
  done = false;
  constructor(workers: number, pipelines: { ops: number }[], buffers: number, filters: number) {
    this.cluster = new ClusterState(workers);
    this.pipelines = pipelines.map((p, i) => new PipelineState(i, p.ops, 0));
    this.buffers = Array.from({ length: buffers }, (_, i) => new BufferState(i, 0));
    this.filters = Array.from({ length: filters }, () => ({ parts: new Map(), merged: new History<FilterKeys | null>(0, null) }));
  }
  split(id: number): SplitState {
    return this.splits[id]!;
  }
  ownersOf(partitioning: number): Record<number, number> {
    return this.owners.get(partitioning)?.now ?? {};
  }
}
