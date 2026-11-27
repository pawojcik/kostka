// The physical operator protocol.
//
// A pipeline is `source -> operator* -> sink`. The physical planner creates one
// PhysicalOperator per position: an immutable spec that knows its input and output
// columns and its configuration, and nothing about any run. A worker creates an
// *instance* of every operator for each split and drives it:
//
//   source.open(ctx, split)
//   while (batch = source.next(ctx)) if (!head.push(ctx, batch)) break
//   head.finish(ctx)          passed down the chain: the split ends, sinks flush
//
// A split is self-contained: instances keep state only while their split runs
// (partial groups, a sort buffer, a top-n heap, counters) and the sink writes all of
// it when the split ends. The one thing that outlives a split is the worker's cache
// (`ctx.cache`: hash tables built from a build side). `push` returns false when the
// consumer wants no more input (a full Limit), which ends the split early.
//
// To add an operator: subclass Operator (or Source / Sink), implement `instance()`
// with the per-split logic and `describe()`, and emit it from the physical planner.

import type { Field } from "../../../core/expr.ts";
import type { Row } from "../../../core/types.ts";
import type { DataFile } from "../../../catalog/meta.ts";
import type { PlanNode } from "../../../sql/plan.ts";
import { Batch } from "./batch.ts";
import type { OutputBuffer, Split } from "./buffer.ts";
import type { Charged, ExecContext } from "./context.ts";
import type { RuntimeFilter } from "./operators/runtime-filter.ts";

export interface OperatorInstance {
  push(ctx: ExecContext, batch: Batch): boolean;
  /** The split ends: flush, release, and pass it on. */
  finish(ctx: ExecContext): void;
  /** What this split's instance held or did, for the workbench. JSON-friendly. */
  state?(): Record<string, unknown>;
}

export interface SourceInstance {
  /** Loads what the split needs before it runs (a data file's bytes); may be async. */
  prepare?(split: Split): Promise<void>;
  open(ctx: ExecContext, split: Split): void;
  next(ctx: ExecContext): Batch | null;
  state?(): Record<string, unknown>;
}

/** What a source reads, for the coordinator to make splits of. */
export type SourceInput =
  /** Data files of a table: one split each, minus those a runtime filter rules out. */
  | { readonly kind: "files"; readonly files: readonly DataFile[]; readonly bytes: (f: DataFile) => number; readonly filters: readonly RuntimeFilter[] }
  /** A buffer's partitions, each partition owned by one worker. `whole`: a split is
   * a whole partition; `always`: partitions read even when empty (a global
   * aggregate's one row). */
  | { readonly kind: "buffer"; readonly buffer: OutputBuffer; readonly whole: boolean; readonly always: readonly number[] };

/** A buffer an operator reads whole, sent with each split: every piece (broadcast),
 * or those of the split's partition. */
export interface WholeInput {
  readonly buffer: OutputBuffer;
  readonly per: "all" | "partition";
}

export abstract class PhysicalOperator {
  readonly input: readonly Field[];
  readonly output: readonly Field[];
  /** The optimized plan node this operator implements (set by the planner). */
  planNode: PlanNode | null = null;
  /** A stable id within the program: "P2.1" is pipeline 2's second operator. */
  id = "";
  constructor(input: readonly Field[], output: readonly Field[]) {
    this.input = input;
    this.output = output;
  }
  /** Short name for plans and the workbench ("HashProbe(inner, 1 key)"). */
  abstract label(): string;
  /** Its configuration, for the workbench. JSON-friendly. */
  describe(): Record<string, unknown> {
    return {};
  }
  /** Buffers it reads (sources, join builds). */
  reads(): readonly OutputBuffer[] {
    return [];
  }
  /** Buffers it reads whole, as inputs of each split. */
  wholeInputs(): readonly WholeInput[] {
    return [];
  }
}

export abstract class Operator extends PhysicalOperator {
  abstract instance(me: Charged, next: OperatorInstance): OperatorInstance;
}

export abstract class Source extends PhysicalOperator {
  constructor(output: readonly Field[]) {
    super([], output);
  }
  abstract reading(): SourceInput;
  abstract instance(me: Charged): SourceInstance;
  /** Its pipeline always has at most one split: a whole partition of a buffer with
   * one partition. */
  oneSplit(): boolean {
    const r = this.reading();
    return r.kind === "buffer" && r.whole && r.buffer.n === 1;
  }
}

export abstract class Sink extends PhysicalOperator {
  constructor(input: readonly Field[]) {
    super(input, []);
  }
  abstract instance(me: Charged): OperatorInstance;
  /** The output buffer it writes, if any. */
  writes(): OutputBuffer | null {
    return null;
  }
}

/** Streaming helper for operator instances. */
export function emit(me: Charged, op: PhysicalOperator, ctx: ExecContext, next: OperatorInstance, rows: Row[]): boolean {
  if (!rows.length) return true;
  const batch = new Batch(op.output, rows);
  me.metrics.gaveOut(batch);
  return next.push(ctx, batch);
}
