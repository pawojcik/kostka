// Exchanges: where the plan is cut into stages, and data moves between tasks.
//
// ShuffleExchange (Spark's sort-based shuffle). Its map side runs in each task of the
// stage below it: the task computes its partition of the child, puts each row in the
// reduce partition its keys hash to (core/hash.ts, the same everywhere), and writes
// one shuffle file: the rows grouped by reduce partition, with an index of where each
// partition starts (`writeMap`). Its read side, in a task of the stage above, is one
// reduce partition: that partition's rows from every map task's file (`execute`).
// A SinglePartition exchange has one reduce partition.
//
// BroadcastExchange. Its child runs as a job of its own (stages.ts), whose tasks send
// their rows to the driver; the read side is the whole value, which each executor
// fetches once and every task there shares (`value`). The joins that read it build
// their hash table (or list of rows) from it.

import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import { COST } from "../../../../core/cost.ts";
import { internal } from "../../../../core/errors.ts";
import { partitionOf } from "../../../../core/hash.ts";
import type { Row } from "../../../../core/types.ts";
import type { TaskContext } from "../context.ts";
import { SparkOp, batches, describePartitioning, partitionCount, type BatchIter, type Partitioning } from "../plan.ts";

export class ShuffleExchange extends SparkOp {
  readonly child: SparkOp;
  readonly target: Exclude<Partitioning, { kind: "unknown" }>;
  /** Its shuffle id: the stage that writes it, and the stages that read it, name it. */
  shuffle = -1;
  private readonly keys: Evaluator[];

  constructor(child: SparkOp, target: Exclude<Partitioning, { kind: "unknown" }>) {
    super(child.output);
    this.child = child;
    this.target = target;
    this.keys = target.kind === "hash" ? target.keys.map((k) => compileExpr(k, child.output)) : [];
  }

  children(): readonly SparkOp[] {
    return [this.child];
  }

  name(): string {
    return "Exchange";
  }

  args(): string {
    return describePartitioning(this.target);
  }

  partitioning(): Partitioning {
    return this.target;
  }

  /** The reduce partitions. */
  get n(): number {
    return partitionCount(this.target);
  }

  /** The map side: partition `partition` of the child, written as one shuffle file. */
  writeMap(ctx: TaskContext, partition: number): void {
    const parts: Row[][] = Array.from({ length: this.n }, () => []);
    const input = this.child.execute(ctx, partition);
    let rows = 0;
    for (let b = input.next(); b; b = input.next()) {
      for (const row of b.rows) parts[this.n === 1 ? 0 : partitionOf(this.keys.map((k) => k(row)), this.n)]!.push(row);
      rows += b.rows.length;
    }
    // Sort-based shuffle: the file is the rows sorted by reduce partition.
    ctx.cpu(rows * COST.partition, this);
    ctx.shuffleWrite(this.shuffle, parts, this);
    ctx.note(this, `wrote a shuffle file: ${rows.toLocaleString("en")} rows in ${parts.filter((p) => p.length).length} of ${this.n} reduce partition(s)`);
  }

  /** The read side: reduce partition `partition`, from every map task. */
  execute(ctx: TaskContext, partition: number): BatchIter {
    const rows = ctx.shuffleRead(this.shuffle, partition, this);
    ctx.cpu(rows.length * COST.exchangeRow, this);
    return batches(ctx, this, rows);
  }
}

export class BroadcastExchange extends SparkOp {
  readonly child: SparkOp;
  /** Its broadcast id: the job that makes it and the stages that read it name it. */
  broadcast = -1;

  constructor(child: SparkOp) {
    super(child.output);
    this.child = child;
  }

  children(): readonly SparkOp[] {
    return [this.child];
  }

  name(): string {
    return "BroadcastExchange";
  }

  args(): string {
    return "";
  }

  partitioning(): Partitioning {
    return { kind: "single" };
  }

  /** The broadcast's value on this task's executor, built by `build` the first time. */
  value<T>(ctx: TaskContext, by: SparkOp, build: (rows: readonly Row[]) => { value: T; bytes: number }): T {
    return ctx.broadcast(this.broadcast, by, build);
  }

  /** A broadcast is read whole by the operator that uses it, never as a stream. */
  execute(): BatchIter {
    throw internal("a BroadcastExchange is read whole (value), not executed");
  }
}
