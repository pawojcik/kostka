// The three joins Spark plans here. In each, the left input streams (the probe side)
// and the right one is the build side, as the optimizer ordered them (the smaller
// side on the right). Join keys never match a null; a left join gives a left row with
// no match once, padded with nulls. `residual` is the rest of the join condition,
// checked on each candidate pair.
//
// - BroadcastHashJoin: the right side is a broadcast. The first task on an executor
//   that needs it builds a hash table from it (core/hash-table.ts), and every task
//   there shares the table.
// - BroadcastNestedLoopJoin: a join without equality keys. The right side is a
//   broadcast, and every left row is checked against every right row.
// - SortMergeJoin: both sides are hash-partitioned by the join keys into the same
//   partitions and sorted by them (the planner adds the Exchanges and the Sorts), so a
//   task walks the two sorted partitions side by side.
//
// A join can multiply rows, so its output comes in batches of at most batchRows.

import { Batch, checkColumns } from "../../../../core/batch.ts";
import { compileExpr, compilePredicate, type Evaluator } from "../../../../core/compile.ts";
import { COST, exprCost } from "../../../../core/cost.ts";
import { SortKey, type Expr } from "../../../../core/expr.ts";
import { HashTable, keyOf } from "../../../../core/hash-table.ts";
import { keyComparator, type KeyCompare } from "../../../../core/row-order.ts";
import { rowsBytes } from "../../../../core/rows.ts";
import type { Row, Value } from "../../../../core/types.ts";
import type { JoinType } from "../../../../sql/plan.ts";
import type { TaskContext } from "../context.ts";
import { SparkOp, type BatchIter, type Partitioning } from "../plan.ts";
import type { BroadcastExchange } from "./exchange.ts";

/** Rows from a generator, as batches of at most ctx.batchRows counted as `op`'s output. */
function batched(ctx: TaskContext, op: SparkOp, rows: Generator<Row>): BatchIter {
  return {
    next: () => {
      const out: Row[] = [];
      while (out.length < ctx.batchRows) {
        const r = rows.next();
        if (r.done) break;
        out.push(r.value);
      }
      if (!out.length) return null;
      ctx.output(op, out.length);
      return new Batch(op.output, out);
    },
  };
}

abstract class JoinExec extends SparkOp {
  readonly left: SparkOp;
  readonly right: SparkOp;
  readonly how: JoinType;
  readonly residualExpr: Expr | null;
  protected readonly residual: ((row: Row) => boolean) | null;
  protected readonly pad: Row;
  constructor(left: SparkOp, right: SparkOp, how: JoinType, residual: Expr | null) {
    super([...left.output, ...right.output]);
    this.left = left;
    this.right = right;
    this.how = how;
    this.residualExpr = residual;
    this.residual = residual ? compilePredicate(residual, this.output) : null;
    this.pad = new Array(right.output.length).fill(null);
  }
  children(): readonly SparkOp[] {
    return [this.left, this.right];
  }
  partitioning(): Partitioning {
    return this.left.partitioning();
  }
  protected joinType(): string {
    return this.how === "left" ? "LeftOuter" : this.how === "cross" ? "Cross" : "Inner";
  }
}

export class BroadcastHashJoinExec extends JoinExec {
  readonly broadcast: BroadcastExchange;
  readonly leftKeys: readonly Expr[];
  readonly rightKeys: readonly Expr[];
  private readonly probeKeys: Evaluator[];
  private readonly buildKeys: Evaluator[];
  constructor(left: SparkOp, right: BroadcastExchange, leftKeys: readonly Expr[], rightKeys: readonly Expr[], how: JoinType, residual: Expr | null) {
    super(left, right, how, residual);
    this.broadcast = right;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.probeKeys = leftKeys.map((k) => compileExpr(k, left.output));
    this.buildKeys = rightKeys.map((k) => compileExpr(k, right.output));
  }
  name(): string {
    return "BroadcastHashJoin";
  }
  args(): string {
    const keys = (ks: readonly Expr[]) => `[${ks.map((k) => k.display()).join(", ")}]`;
    return `${keys(this.leftKeys)}, ${keys(this.rightKeys)}, ${this.joinType()}, BuildRight${this.residualExpr ? `, ${this.residualExpr.display()}` : ""}`;
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const table = this.broadcast.value(ctx, this, (rows) => {
      const t = new HashTable();
      ctx.cpu(rows.length * COST.hashInsert, this);
      t.add(rows, this.buildKeys);
      ctx.note(this, `built the broadcast hash table: ${t.rows.toLocaleString("en")} rows, ${t.buckets.size.toLocaleString("en")} keys`);
      return { value: t, bytes: t.bytes };
    });
    const input = this.left.execute(ctx, partition);
    const self = this;
    function* rows(): Generator<Row> {
      for (let b = input.next(); b; b = input.next()) {
        checkColumns(b, self.left.output, "BroadcastHashJoin");
        let out = 0;
        for (const l of b.rows) {
          const k = keyOf(l, self.probeKeys);
          const bucket = k === undefined ? undefined : table.buckets.get(k);
          let matched = false;
          for (const r of bucket ?? []) {
            const row = l.concat(r);
            if (self.residual && !self.residual(row)) continue;
            matched = true;
            out++;
            yield row;
          }
          if (!matched && self.how === "left") {
            out++;
            yield l.concat(self.pad);
          }
        }
        ctx.cpu(b.rows.length * COST.hashProbe + out * COST.joinOutput, self);
      }
    }
    return batched(ctx, this, rows());
  }
}

export class BroadcastNestedLoopJoinExec extends JoinExec {
  readonly broadcast: BroadcastExchange;
  private readonly cost: number;
  constructor(left: SparkOp, right: BroadcastExchange, how: JoinType, condition: Expr | null) {
    super(left, right, how, condition);
    this.broadcast = right;
    this.cost = condition ? exprCost(condition) : 0;
  }
  name(): string {
    return "BroadcastNestedLoopJoin";
  }
  args(): string {
    return `BuildRight, ${this.joinType()}${this.residualExpr ? `, ${this.residualExpr.display()}` : ""}`;
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const right = this.broadcast.value(ctx, this, (rows) => ({ value: rows, bytes: rowsBytes(rows) }));
    const input = this.left.execute(ctx, partition);
    const self = this;
    function* rows(): Generator<Row> {
      for (let b = input.next(); b; b = input.next()) {
        checkColumns(b, self.left.output, "BroadcastNestedLoopJoin");
        let out = 0;
        for (const l of b.rows) {
          let matched = false;
          for (const r of right) {
            const row = l.concat(r);
            if (self.residual && !self.residual(row)) continue;
            matched = true;
            out++;
            yield row;
          }
          if (!matched && self.how === "left") {
            out++;
            yield l.concat(self.pad);
          }
        }
        ctx.cpu(b.rows.length * right.length * (COST.row + self.cost) + out * COST.joinOutput, self);
      }
    }
    return batched(ctx, this, rows());
  }
}

/** One row at a time from a stream of batches. */
class Cursor {
  private batch: readonly Row[] = [];
  private pos = 0;
  private readonly input: BatchIter;
  constructor(input: BatchIter) {
    this.input = input;
  }
  peek(): Row | null {
    while (this.pos >= this.batch.length) {
      const b = this.input.next();
      if (!b) return null;
      this.batch = b.rows;
      this.pos = 0;
    }
    return this.batch[this.pos]!; // the loop above leaves pos inside the batch
  }
  advance(): void {
    this.pos++;
  }
}

/** The sort keys a SortMergeJoin's inputs are sorted by: ascending, nulls first. */
export function joinSortKeys(keys: readonly Expr[]): SortKey[] {
  return keys.map((k) => new SortKey(k, true, true));
}

export class SortMergeJoinExec extends JoinExec {
  readonly leftKeys: readonly Expr[];
  readonly rightKeys: readonly Expr[];
  private readonly lk: Evaluator[];
  private readonly rk: Evaluator[];
  private readonly cmp: KeyCompare;
  constructor(left: SparkOp, right: SparkOp, leftKeys: readonly Expr[], rightKeys: readonly Expr[], how: JoinType, residual: Expr | null) {
    super(left, right, how, residual);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.lk = leftKeys.map((k) => compileExpr(k, left.output));
    this.rk = rightKeys.map((k) => compileExpr(k, right.output));
    this.cmp = keyComparator(joinSortKeys(leftKeys));
  }
  name(): string {
    return "SortMergeJoin";
  }
  args(): string {
    const keys = (ks: readonly Expr[]) => `[${ks.map((k) => k.display()).join(", ")}]`;
    return `${keys(this.leftKeys)}, ${keys(this.rightKeys)}, ${this.joinType()}${this.residualExpr ? `, ${this.residualExpr.display()}` : ""}`;
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const L = new Cursor(this.left.execute(ctx, partition));
    const R = new Cursor(this.right.execute(ctx, partition));
    const self = this;
    const key = (row: Row, evals: readonly Evaluator[]): Value[] | null => {
      const k: Value[] = [];
      for (const e of evals) {
        const v = e(row);
        if (v === null) return null;
        k.push(v);
      }
      return k;
    };
    let compared = 0, out = 0;
    const charge = () => {
      ctx.cpu(compared * COST.sortCompare + out * COST.joinOutput, self);
      compared = out = 0;
    };
    function* rows(): Generator<Row> {
      for (let l = L.peek(); l; l = L.peek()) {
        const lk = key(l, self.lk);
        if (!lk) {
          if (self.how === "left") yield l.concat(self.pad);
          L.advance();
          continue;
        }
        // Skip right rows with a smaller key (or a null one).
        let r = R.peek();
        for (; r; r = R.peek()) {
          const rk = key(r, self.rk);
          compared++;
          if (rk && self.cmp(rk, lk) >= 0) break;
          R.advance();
        }
        const rk = r ? key(r, self.rk) : null;
        if (!r || !rk || self.cmp(rk, lk) !== 0) {
          if (self.how === "left") yield l.concat(self.pad);
          L.advance();
          continue;
        }
        // The right rows with this key, then every left row with it.
        const group: Row[] = [];
        for (let x = R.peek(); x; x = R.peek()) {
          const xk = key(x, self.rk);
          if (!xk || self.cmp(xk, lk) !== 0) break;
          group.push(x);
          R.advance();
        }
        for (let y = L.peek(); y; y = L.peek()) {
          const yk = key(y, self.lk);
          if (!yk || self.cmp(yk, lk) !== 0) break;
          compared++;
          let matched = false;
          for (const g of group) {
            const row = y.concat(g);
            if (self.residual && !self.residual(row)) continue;
            matched = true;
            out++;
            yield row;
          }
          if (!matched && self.how === "left") yield y.concat(self.pad);
          L.advance();
        }
        charge();
      }
      charge();
    }
    return batched(ctx, this, rows());
  }
}
