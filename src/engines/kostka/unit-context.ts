// The context one split runs in on a simulated worker: the adapter between kostka's
// worker (the domain) and the cluster (the infrastructure). It reports the split's
// CPU and IO time to its unit on the node (cluster/node.ts), which places it on a
// core's timeline and prices IO; it fetches pieces through the data plane; and it
// passes the rest (pieces written, memory, inputs, filters, the cache, results) to
// the worker.
//
// CPU time is either the cost model's (operators report it with `cpu()`), or
// measured: every operator instance is wrapped (`timed`), and the context keeps a
// stack of the operators running. The time between two events (an operator called
// or returning, or IO charged) goes to the operator on top, so each operator gets its
// *own* time, not that of the operators it pushes rows to, and CPU and IO segments
// come out in the order they happened.

import { internal } from "../../core/errors.ts";
import type { Row, Value } from "../../core/types.ts";
import type { OutputBuffer, PieceRef, Split } from "./physical/buffer.ts";
import type { Charged, HoldKind } from "./physical/context.ts";
import { rowsBytes } from "../../cluster/rows.ts";
import type { OperatorInstance, SourceInstance } from "./physical/operator.ts";
import type { CpuMode } from "../../cluster/config.ts";
import type { IoCategory, ByteCounter, Unit } from "../../cluster/node.ts";
import { BlockLostError, type DataPlane } from "../../cluster/blocks.ts";
import { PieceLostError } from "./protocol.ts";
import type { SplitHooks, UnitContext, Work } from "./worker/worker.ts";

export class RunContext implements UnitContext {
  readonly worker: number;
  readonly split: Split;
  readonly pipeline: number;
  readonly batchRows: number;
  readonly notes = new Map<Charged, string[]>();
  private readonly unit: Unit;
  private current: Charged | null = null;
  readonly measured: boolean;
  /** Measured mode: the operators running now, innermost last, and when the time
   * since was last charged. */
  private readonly running: Charged[] = [];
  private mark = 0;
  private readonly hooks: SplitHooks;
  private readonly plane: DataPlane;

  constructor(unit: Unit, worker: number, work: Work, hooks: SplitHooks, batchRows: number, cpu: CpuMode, plane: DataPlane) {
    this.measured = cpu === "measured";
    this.unit = unit;
    this.worker = worker;
    this.split = work.assigned.split;
    this.pipeline = work.pipeline.index;
    this.batchRows = batchRows;
    this.hooks = hooks;
    this.plane = plane;
    if (this.measured) this.mark = performance.now();
  }

  /** Now, in ns, on the core's timeline. */
  get cursor(): number {
    return this.unit.cursor;
  }

  private addCpu(ns: number, op: Charged | null): void {
    const took = this.unit.cpu(ns);
    if (op) op.metrics.time.cpu += took;
  }

  private addIo(kind: IoCategory, bytes: number, counter: ByteCounter, op: Charged | null): void {
    this.lap();
    const took = this.unit.io(kind, bytes, counter);
    if (op) op.metrics.time[kind] += took;
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
    if (!this.measured) this.addCpu(Math.round(ns), op);
  }

  cpuTaken(ns: number, op: Charged): void {
    if (!this.measured) return;
    this.lap();
    this.addCpu(ns, op);
  }

  /** Measured mode: charges the time since the last event to the operator on top. */
  private lap(): void {
    if (!this.measured) return;
    const now = performance.now();
    const top = this.running[this.running.length - 1];
    if (top) this.addCpu(Math.round((now - this.mark) * 1e6), top);
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
    this.addIo("storage", bytes, "storage_read", op);
  }

  pieces(): readonly PieceRef[] {
    return this.hooks.pieces();
  }

  fetch(ref: PieceRef, op: Charged): readonly Row[] {
    const block = this.aside(() => {
      try {
        return this.plane.read(ref.worker, ref.id);
      } catch (e) {
        // the worker's view of a block gone with its node
        if (e instanceof BlockLostError) throw new PieceLostError(ref);
        throw e;
      }
    });
    const share = ref.bytes;
    if (block.spilled) this.addIo("spill_read", share, "spill_read", op);
    if (ref.worker !== this.worker) this.addIo("network", share, "network_in", op);
    op.metrics.bytesIn += share;
    return block.rows;
  }

  store(buffer: OutputBuffer, partition: number, rows: Row[], op: Charged): PieceRef {
    const ref = this.aside(() => this.as(op, () => this.hooks.store(buffer, partition, rows, this)));
    op.metrics.bytesOut += ref.bytes;
    return ref;
  }

  send(rows: readonly Row[], op: Charged): void {
    const bytes = rowsBytes(rows);
    op.metrics.bytesOut += bytes;
    this.addIo("network", bytes, "network_out", op);
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
    this.addIo("spill_write", bytes, "spill_written", this.current);
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

  end(finished: boolean) {
    this.lap();
    const t = this.unit.end(finished);
    return { start: t.start, end: t.end, time: t.time, segments: t.segments, notes: this.notes };
  }
}

/** Runs `fn` as `op`'s own CPU time, in measured mode. Instances only ever run in a
 * RunContext (the worker's ports make both). */
function asOp<T>(op: Charged, ctx: unknown, fn: () => T): T {
  if (!(ctx instanceof RunContext)) throw internal("a timed operator ran outside a RunContext");
  ctx.enter(op);
  try {
    return fn();
  } finally {
    ctx.exit();
  }
}

/** An operator instance whose calls are timed as `op`'s CPU in measured mode. */
export function timed(op: Charged, inst: OperatorInstance): OperatorInstance {
  return {
    push: (ctx, batch) => asOp(op, ctx, () => inst.push(ctx, batch)),
    finish: (ctx) => asOp(op, ctx, () => inst.finish(ctx)),
    state: inst.state,
  };
}

export function timedSource(op: Charged, inst: SourceInstance): SourceInstance {
  return {
    prepare: inst.prepare,
    open: (ctx, split) => asOp(op, ctx, () => inst.open(ctx, split)),
    next: (ctx) => asOp(op, ctx, () => inst.next(ctx)),
    state: inst.state,
  };
}
