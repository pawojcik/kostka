// Sorting.
//
// Sort: sorts its partition. It buffers rows while they fit in the task's memory
// share (memoryMB / cores); when the next batch does not fit, it sorts what it holds
// into a run and spills the run to the node's local disk (Spark's external sorter).
// At the end it reads the spilled runs back and merges them k-way with the rows still
// in memory. A Sort over the one partition of an ORDER BY is global; the two Sorts
// under a SortMergeJoin sort each partition by the join keys.
//
// TakeOrderedAndProject: Spark's ORDER BY ... LIMIT k. Each task keeps the best k rows
// of its partition in a bounded heap (partial); a single task then keeps the best k
// of those (final). Rows with equal keys may come out in any order.

import { checkColumns } from "../../../../core/batch.ts";
import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import { COST, sortCost } from "../../../../core/cost.ts";
import type { SortKey } from "../../../../core/expr.ts";
import { TopK, bestRun, keyComparator, type KeyCompare, type Keyed } from "../../../../core/row-order.ts";
import { perRowBytes } from "../../../../core/rows.ts";
import type { Row } from "../../../../core/types.ts";
import type { SpilledRun, TaskContext } from "../context.ts";
import { SparkOp, batches, type BatchIter, type Partitioning } from "../plan.ts";

abstract class Ordering extends SparkOp {
  readonly child: SparkOp;
  readonly keys: readonly SortKey[];
  protected readonly evals: Evaluator[];
  protected readonly cmp: KeyCompare;
  constructor(child: SparkOp, keys: readonly SortKey[]) {
    super(child.output);
    this.child = child;
    this.keys = keys;
    this.evals = keys.map((k) => compileExpr(k.expr, child.output));
    this.cmp = keyComparator(keys);
  }
  children(): readonly SparkOp[] {
    return [this.child];
  }
  protected keyed(row: Row): Keyed {
    return { k: this.evals.map((e) => e(row)), row };
  }
  protected keyText(): string {
    return this.keys.map((k) => `${k.expr.display()} ${k.ascending ? "ASC" : "DESC"} NULLS ${k.nullsFirst ? "FIRST" : "LAST"}`).join(", ");
  }
}

export class SortExec extends Ordering {
  /** Sorts the query's one partition (ORDER BY), not each partition (a join's input). */
  readonly global: boolean;
  constructor(child: SparkOp, keys: readonly SortKey[], global: boolean) {
    super(child, keys);
    this.global = global;
  }
  name(): string {
    return "Sort";
  }
  args(): string {
    return `[${this.keyText()}], ${this.global}`;
  }
  partitioning(): Partitioning {
    return this.child.partitioning();
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    let buf: Keyed[] = [];
    const runs: SpilledRun[] = [];
    let perRow = 0, total = 0;
    const sorted = (items: Keyed[]) => {
      ctx.cpu(sortCost(items.length), this);
      return items.sort((a, b) => this.cmp(a.k, b.k));
    };
    for (let b = input.next(); b; b = input.next()) {
      checkColumns(b, this.child.output, "Sort");
      for (const row of b.rows) buf.push(this.keyed(row));
      total += b.rows.length;
      ctx.cpu(b.rows.length * (COST.row + this.evals.length * COST.exprNode), this);
      if (!perRow && b.rows.length) perRow = perRowBytes(b.rows);
      if (!ctx.reserve(this, "sort buffer", Math.round(perRow * buf.length))) {
        // It doesn't fit: sort what is held into a run, and spill the run.
        runs.push(ctx.spill(sorted(buf).map((x) => x.row), this));
        buf = [];
        ctx.free(this);
      }
    }
    const inMemory = sorted(buf);
    if (!runs.length) {
      ctx.free(this);
      return batches(ctx, this, inMemory.map((x) => x.row));
    }
    // Merge the spilled runs (read back from disk) with the rows still in memory.
    const all: Keyed[][] = [...runs.map((r) => ctx.unspill(r, this).map((row) => this.keyed(row))), inMemory];
    const heads = all.map(() => 0);
    const out: Row[] = [];
    for (let i = bestRun(all, heads, this.cmp); i >= 0; i = bestRun(all, heads, this.cmp)) out.push(all[i]![heads[i]!++]!.row);
    ctx.cpu(total * Math.max(1, Math.log2(all.length + 1)) * COST.sortCompare, this);
    ctx.note(this, `merged ${runs.length} spilled run(s) and ${inMemory.length.toLocaleString("en")} row(s) in memory`);
    ctx.free(this);
    return batches(ctx, this, out);
  }
}

export class TakeOrderedAndProjectExec extends Ordering {
  readonly limit: number;
  readonly mode: "partial" | "final";
  constructor(child: SparkOp, keys: readonly SortKey[], limit: number, mode: "partial" | "final") {
    super(child, keys);
    this.limit = limit;
    this.mode = mode;
  }
  name(): string {
    return "TakeOrderedAndProject";
  }
  args(): string {
    return `(limit=${this.limit}, orderBy=[${this.keyText()}]${this.mode === "partial" ? ", partial" : ""})`;
  }
  partitioning(): Partitioning {
    return this.child.partitioning();
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    const heap = new TopK(this.limit, this.cmp);
    for (let b = input.next(); b; b = input.next()) {
      checkColumns(b, this.child.output, "TakeOrderedAndProject");
      for (const row of b.rows) heap.offer(this.keyed(row));
      ctx.cpu(b.rows.length * (COST.row + this.evals.length * COST.exprNode + Math.log2(this.limit + 1) * COST.sortCompare), this);
    }
    return batches(ctx, this, heap.take().map((x) => x.row));
  }
}
