// The Spark plan: a tree of physical operators, as Spark's SparkPlan is. Each operator
// is a spec that does not change once planned: its children, its output columns, how
// its output is partitioned, and `execute(ctx, partition)`, which computes one
// partition of its output as a stream of batches, pulling from its children (Spark's
// iterator model). Two things are known only once the whole tree is built: its number
// in the plan and the plan node it implements. The planner sets each once (`place`),
// and a SparkPlan checks that every operator has them.
//
// Exchanges are where data moves between tasks:
// - ShuffleExchange: its map side (a task of the stage below it) writes one shuffle
//   file; its read side (`execute`) reads one reduce partition from every map task.
// - BroadcastExchange: its child runs as a job of its own; the driver collects its
//   rows, and the read side is the whole value, on every executor.
// The plan is cut into stages at the exchanges (stages.ts). Operators between two
// exchanges run in one task, batch by batch: one WholeStageCodegen box.
//
// To add an operator: subclass SparkOp, implement `execute`, `partitioning`, `name`
// and `args`, and emit it from the planner.

import { Batch } from "../../../core/batch.ts";
import { internal } from "../../../core/errors.ts";
import { ColumnRef, type Expr, type Field } from "../../../core/expr.ts";
import type { Row } from "../../../core/types.ts";
import type { PlanNode } from "../../../sql/plan.ts";
import type { Charged, TaskContext } from "./context.ts";

/** How an operator's output rows are spread over partitions. */
export type Partitioning =
  /** `n` partitions with no known key: a FileScan's files. */
  | { readonly kind: "unknown"; readonly n: number }
  /** By a stable hash of `keys` (core/hash.ts: partitionOf) into `n` partitions. */
  | { readonly kind: "hash"; readonly keys: readonly Expr[]; readonly n: number }
  /** Everything in one partition. */
  | { readonly kind: "single" };

export function partitionCount(p: Partitioning): number {
  return p.kind === "single" ? 1 : p.n;
}

/** Two expressions that are the same column. */
const sameColumn = (a: Expr, b: Expr) => a instanceof ColumnRef && b instanceof ColumnRef && a.id === b.id;

/** The partitioning is by exactly these key columns, in this order, into `n`. */
export function hashedBy(p: Partitioning, keys: readonly Expr[], n: number): boolean {
  return p.kind === "hash" && p.n === n && p.keys.length === keys.length && p.keys.every((k, i) => {
    const other = keys[i];
    return other !== undefined && sameColumn(k, other);
  });
}

export function describePartitioning(p: Partitioning): string {
  switch (p.kind) {
    case "unknown": return `UnknownPartitioning(${p.n})`;
    case "hash": return `hashpartitioning(${p.keys.map((k) => k.display()).join(", ")}, ${p.n})`;
    case "single": return "SinglePartition";
    default: {
      const never: never = p;
      return never;
    }
  }
}

/** One partition of an operator's output, a batch at a time. */
export interface BatchIter {
  next(): Batch | null;
}

export abstract class SparkOp implements Charged {
  private number = -1;
  private implemented: PlanNode | null = null;
  readonly output: readonly Field[];
  constructor(output: readonly Field[]) {
    this.output = output;
  }
  /** Its number in the plan (preorder from 0); -1 until the planner numbers it. */
  get id(): number {
    return this.number;
  }
  /** The optimized plan node it implements; null until the planner says which. */
  get planNode(): PlanNode | null {
    return this.implemented;
  }
  /** Sets its number, or the plan node it implements: once each, by the planner. */
  place(p: { readonly id: number } | { readonly node: PlanNode }): void {
    if ("id" in p) {
      if (this.number >= 0) throw internal(`${this.name()} is numbered twice`);
      this.number = p.id;
    } else {
      if (this.implemented) throw internal(`${this.name()} is given its plan node twice`);
      this.implemented = p.node;
    }
  }
  abstract children(): readonly SparkOp[];
  /** Spark's name for it ("HashAggregate"). */
  abstract name(): string;
  /** Its arguments, as Spark's explain shows them ("keys=[city], functions=[count(1)]"). */
  abstract args(): string;
  abstract partitioning(): Partitioning;
  /** Computes partition `partition` of its output. */
  abstract execute(ctx: TaskContext, partition: number): BatchIter;
  label(): string {
    const a = this.args();
    return a ? `${this.name()} ${a}` : this.name();
  }
}

/** Rows as batches of at most ctx.batchRows, counted as `op`'s output. */
export function batches(ctx: TaskContext, op: SparkOp, rows: readonly Row[]): BatchIter {
  let pos = 0;
  return {
    next: () => {
      if (pos >= rows.length) return null;
      const out = rows.slice(pos, pos + ctx.batchRows);
      pos += out.length;
      ctx.output(op, out.length);
      return new Batch(op.output, out);
    },
  };
}

/** Every row of a stream, read to its end. */
export function drain(it: BatchIter): Row[] {
  const out: Row[] = [];
  for (let b = it.next(); b; b = it.next()) for (const r of b.rows) out.push(r);
  return out;
}

/** Every operator of a plan, parents before children. */
export function* walk(op: SparkOp): Generator<SparkOp> {
  yield op;
  for (const c of op.children()) yield* walk(c);
}

/** The plan as Spark's explain prints it: an indented tree. */
export function explainTree(root: SparkOp): string {
  const lines: string[] = [];
  const visit = (op: SparkOp, depth: number) => {
    lines.push(`${"  ".repeat(depth)}${depth ? "+- " : ""}${op.label()}`);
    for (const c of op.children()) visit(c, depth + 1);
  };
  visit(root, 0);
  return lines.join("\n");
}
