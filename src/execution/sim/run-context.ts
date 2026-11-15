// The context one split runs in on a simulated worker: infrastructure. It lays the
// split's CPU and IO time out on the worker's timeline as segments [kind, start,
// end], prices IO with the IO profile, fetches pieces through the data plane, and
// passes the rest (pieces written, memory, inputs, filters, the cache, results) to
// the worker's domain.
//
// CPU time is either the cost model's (operators report it with `cpu()`), or
// measured: every operator instance is wrapped (`timed`), and the context keeps a
// stack of the operators running. The time between two events (an operator called
// or returning, or IO charged) goes to the operator on top, so each operator gets its
// *own* time, not that of the operators it pushes rows to, and CPU and IO segments
// come out in the order they happened.

import type { Row, Value } from "../../core/types.ts";
import type { OutputBuffer, PieceRef, Split } from "../../physical/buffer.ts";
import { CATEGORIES, type Category, type Charged, type HoldKind } from "../../physical/context.ts";
import { rowsBytes } from "../../physical/metrics.ts";
import type { OperatorInstance, SourceInstance } from "../../physical/operator.ts";
import { ioCost, type CpuMode, type IOProfile } from "../cluster.ts";
import type { LocalPiece } from "../worker/buffers.ts";
import type { SplitHooks, UnitContext, Work } from "../worker/worker.ts";

export type Segment = [kind: Category, start: number, end: number];

/** A simulated machine: its clock and where its time and bytes went. */
export class Machine {
  readonly id: number;
  readonly profile: IOProfile;
  /** Simulated ns: when it is free for its next unit. */
  clock = 0;
  runs = 0;
  readonly time: Record<Category, number> = Object.fromEntries(CATEGORIES.map((c) => [c, 0])) as Record<Category, number>;
  readonly bytes = { storage_read: 0, network_in: 0, network_out: 0, spill_written: 0, spill_read: 0 };
  constructor(id: number, profile: IOProfile) {
    this.id = id;
    this.profile = profile;
  }
  get busy(): number {
    return CATEGORIES.reduce((s, c) => s + this.time[c], 0);
  }
}

/** Reads a piece from the worker holding it (PieceLostError if that worker is gone). */
export type DataPlane = (ref: PieceRef) => LocalPiece;

export class RunContext implements UnitContext {
  readonly m: Machine;
  readonly worker: number;
  readonly split: Split;
  readonly pipeline: number;
  readonly batchRows: number;
  readonly start: number;
  cursor: number;
  readonly segments: Segment[] = [];
  readonly time: Record<Category, number> = Object.fromEntries(CATEGORIES.map((c) => [c, 0])) as Record<Category, number>;
  readonly notes = new Map<Charged, string[]>();
  private current: Charged | null = null;
  readonly measured: boolean;
  /** Measured mode: the operators running now, innermost last, and when the time
   * since was last charged. */
  private readonly running: Charged[] = [];
  private mark = 0;
  private readonly hooks: SplitHooks;
  private readonly plane: DataPlane;

  constructor(m: Machine, work: Work, hooks: SplitHooks, start: number, batchRows: number, cpu: CpuMode, plane: DataPlane) {
    this.measured = cpu === "measured";
    this.m = m;
    this.worker = m.id;
    this.split = work.assigned.split;
    this.pipeline = work.pipeline.index;
    this.batchRows = batchRows;
    this.start = this.cursor = start;
    this.hooks = hooks;
    this.plane = plane;
    if (this.measured) this.mark = performance.now();
  }

  private add(kind: Category, ns: number, op: Charged | null): void {
    if (kind !== "cpu") this.lap();
    if (ns <= 0) return;
    const last = this.segments[this.segments.length - 1];
    if (last && last[0] === kind && last[2] === this.cursor) last[2] += ns;
    else this.segments.push([kind, this.cursor, this.cursor + ns]);
    this.cursor += ns;
    this.time[kind] += ns;
    if (op) op.metrics.time[kind] += ns;
  }

  private as<T>(op: Charged, fn: () => T): T {
    const prev = this.current;
    this.current = op;
    try {
      return fn();
    } finally {
      this.current = prev;
    }
  }

  cpu(ns: number, op: Charged): void {
    if (!this.measured) this.add("cpu", Math.round(ns), op);
  }

  cpuTaken(ns: number, op: Charged): void {
    if (!this.measured) return;
    this.lap();
    this.add("cpu", ns, op);
  }

  /** Measured mode: charges the time since the last event to the operator on top. */
  private lap(): void {
    if (!this.measured) return;
    const now = performance.now();
    const top = this.running[this.running.length - 1];
    if (top) this.add("cpu", Math.round((now - this.mark) * 1e6), top);
    this.mark = now;
  }

  enter(op: Charged): void {
    if (!this.measured) return;
    this.lap();
    this.running.push(op);
  }

  exit(): void {
    if (!this.measured) return;
    this.lap();
    this.running.pop();
  }

  /** Domain bookkeeping (messages, memory) is not charged as CPU time. */
  private aside<T>(fn: () => T): T {
    this.lap();
    try {
      return fn();
    } finally {
      if (this.measured) this.mark = performance.now();
    }
  }

  readStorage(bytes: number, op: Charged): void {
    this.m.bytes.storage_read += bytes;
    this.add("storage", ioCost(this.m.profile, "storage", bytes), op);
  }

  pieces(): readonly PieceRef[] {
    return this.hooks.pieces();
  }

  fetch(ref: PieceRef, op: Charged): readonly Row[] {
    const piece = this.aside(() => this.plane(ref));
    const share = ref.bytes;
    if (piece.spilled) {
      this.m.bytes.spill_read += share;
      this.add("spill_read", ioCost(this.m.profile, "disk", share), op);
    }
    if (ref.worker !== this.worker) {
      this.m.bytes.network_in += share;
      this.add("network", ioCost(this.m.profile, "network", share), op);
    }
    op.metrics.bytesIn += share;
    return piece.rows;
  }

  store(buffer: OutputBuffer, partition: number, rows: Row[], op: Charged): PieceRef {
    const ref = this.aside(() => this.as(op, () => this.hooks.store(buffer, partition, rows, this)));
    op.metrics.bytesOut += ref.bytes;
    return ref;
  }

  send(rows: readonly Row[], op: Charged): void {
    const bytes = rowsBytes(rows);
    this.m.bytes.network_out += bytes;
    op.metrics.bytesOut += bytes;
    this.add("network", ioCost(this.m.profile, "network", bytes), op);
    this.aside(() => this.hooks.send(rows));
  }

  input(buffer: number, partition: number): readonly PieceRef[] {
    return this.hooks.input(buffer, partition);
  }

  filterKeys(filter: number): ReadonlySet<Value> | null {
    return this.hooks.filterKeys(filter);
  }

  collectedKeys(filter: number, keys: Iterable<Value>): void {
    this.hooks.collectedKeys(filter, keys);
  }

  cache<T>(buffer: number, key: string, build: (holdKey: string) => T): { value: T; hit: boolean } {
    return this.hooks.cache(buffer, key, build);
  }

  hold(key: string, kind: HoldKind, label: string, bytes: number, op: Charged, hint?: string): void {
    this.aside(() => this.as(op, () => this.hooks.hold(this, key, kind, label, bytes, hint)));
  }

  grow(key: string, bytes: number, op: Charged): void {
    this.aside(() => this.as(op, () => this.hooks.grow(this, key, bytes)));
  }

  release(key: string): void {
    this.hooks.release(key);
  }

  spillWrite(bytes: number): void {
    this.m.bytes.spill_written += bytes;
    this.add("spill_write", ioCost(this.m.profile, "disk", bytes), this.current);
  }

  spilled(piece: LocalPiece): void {
    this.hooks.spilled(piece);
  }

  tableBuilt(op: Charged, info: { partition: number | null; rows: number; keys: number; bytes: number; again: boolean }, text: string): void {
    void op;
    void info;
    this.hooks.tableBuilt(text);
  }

  note(op: Charged, text: string): void {
    const list = this.notes.get(op) ?? [];
    if (list.length < 4) list.push(text);
    this.notes.set(op, list);
  }

  end() {
    this.lap();
    return { start: this.start, end: this.cursor, time: { ...this.time }, segments: this.segments, notes: this.notes };
  }
}

/** An operator instance whose calls are timed as `op`'s CPU in measured mode. */
export function timed(op: Charged, inst: OperatorInstance): OperatorInstance {
  const run = <T>(ctx: unknown, fn: () => T): T => {
    const c = ctx as RunContext;
    c.enter(op);
    try {
      return fn();
    } finally {
      c.exit();
    }
  };
  return {
    push: (ctx, batch) => run(ctx, () => inst.push(ctx, batch)),
    finish: (ctx) => run(ctx, () => inst.finish(ctx)),
    state: inst.state,
  };
}

export function timedSource(op: Charged, inst: SourceInstance): SourceInstance {
  const run = <T>(ctx: unknown, fn: () => T): T => {
    const c = ctx as RunContext;
    c.enter(op);
    try {
      return fn();
    } finally {
      c.exit();
    }
  };
  return {
    prepare: inst.prepare,
    open: (ctx, split) => run(ctx, () => inst.open(ctx, split)),
    next: (ctx) => run(ctx, () => inst.next(ctx)),
    state: inst.state,
  };
}
