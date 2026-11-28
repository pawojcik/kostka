// Operators that stream: Filter, Project, LocalLimit and GlobalLimit. Each passes its
// child's partitioning on (Project only if the hash keys survive it).
//
// A limit is two operators, as in Spark: LocalLimit keeps n rows of each partition,
// and GlobalLimit, in a stage that reads a single partition, keeps n of those. Where
// the child is one partition already, GlobalLimit alone does. Without an ORDER BY
// below it, which rows it keeps depends on the schedule.

import { Batch, checkColumns } from "../../../../core/batch.ts";
import { compileExpr, compilePredicate, type Evaluator } from "../../../../core/compile.ts";
import { COST, exprCost } from "../../../../core/cost.ts";
import { Alias, ColumnRef, type Expr, type Field } from "../../../../core/expr.ts";
import type { Row } from "../../../../core/types.ts";
import type { TaskContext } from "../context.ts";
import { SparkOp, type BatchIter, type Partitioning } from "../plan.ts";

export class FilterExec extends SparkOp {
  readonly child: SparkOp;
  readonly condition: Expr;
  private readonly test: (row: Row) => boolean;
  private readonly cost: number;
  constructor(child: SparkOp, condition: Expr) {
    super(child.output);
    this.child = child;
    this.condition = condition;
    this.test = compilePredicate(condition, child.output);
    this.cost = exprCost(condition) + COST.row;
  }
  children(): readonly SparkOp[] {
    return [this.child];
  }
  name(): string {
    return "Filter";
  }
  args(): string {
    return this.condition.display();
  }
  partitioning(): Partitioning {
    return this.child.partitioning();
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    return {
      next: () => {
        for (let b = input.next(); b; b = input.next()) {
          checkColumns(b, this.child.output, "Filter");
          ctx.cpu(b.rows.length * this.cost, this);
          const rows = b.rows.filter(this.test);
          if (!rows.length) continue;
          ctx.output(this, rows.length);
          return new Batch(this.output, rows);
        }
        return null;
      },
    };
  }
}

export class ProjectExec extends SparkOp {
  readonly child: SparkOp;
  readonly exprs: readonly Expr[];
  private readonly evals: Evaluator[];
  private readonly cost: number;
  constructor(child: SparkOp, exprs: readonly Expr[], output: readonly Field[]) {
    super(output);
    this.child = child;
    this.exprs = exprs;
    this.evals = exprs.map((e) => compileExpr(e, child.output));
    this.cost = exprs.reduce<number>((s, e) => s + exprCost(e), COST.row);
  }
  children(): readonly SparkOp[] {
    return [this.child];
  }
  name(): string {
    return "Project";
  }
  args(): string {
    return `[${this.exprs.map((e) => e.display()).join(", ")}]`;
  }
  /** The child's hash partitioning, renamed to this operator's output columns; unknown
   * if a key column does not pass through. */
  partitioning(): Partitioning {
    const p = this.child.partitioning();
    if (p.kind !== "hash") return p;
    const keys: Expr[] = [];
    for (const k of p.keys) {
      const i = k instanceof ColumnRef ? this.exprs.findIndex((e) => {
        const inner = e instanceof Alias ? e.child : e;
        return inner instanceof ColumnRef && inner.id === k.id;
      }) : -1;
      if (i < 0) return { kind: "unknown", n: p.n };
      keys.push(this.output[i]!.ref());
    }
    return { kind: "hash", keys, n: p.n };
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    const evals = this.evals;
    return {
      next: () => {
        const b = input.next();
        if (!b) return null;
        checkColumns(b, this.child.output, "Project");
        ctx.cpu(b.rows.length * this.cost, this);
        const out = new Array<Row>(b.rows.length);
        for (let i = 0; i < out.length; i++) {
          const row = b.rows[i]!;
          const r = new Array(evals.length);
          for (let j = 0; j < evals.length; j++) r[j] = evals[j]!(row);
          out[i] = r;
        }
        ctx.output(this, out.length);
        return new Batch(this.output, out);
      },
    };
  }
}

/** Keeps the first n rows of its partition (LocalLimit), or of the one partition
 * there is (GlobalLimit). It stops pulling from its child once it has them. */
abstract class LimitExec extends SparkOp {
  readonly child: SparkOp;
  readonly n: number;
  constructor(child: SparkOp, n: number) {
    super(child.output);
    this.child = child;
    this.n = n;
  }
  children(): readonly SparkOp[] {
    return [this.child];
  }
  args(): string {
    return String(this.n);
  }
  partitioning(): Partitioning {
    return this.child.partitioning();
  }
  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    let left = this.n;
    return {
      next: () => {
        if (left <= 0) return null;
        const b = input.next();
        if (!b) return null;
        const rows = b.rows.slice(0, left);
        left -= rows.length;
        ctx.cpu(rows.length * COST.row, this);
        ctx.output(this, rows.length);
        return new Batch(this.output, rows);
      },
    };
  }
}

export class LocalLimitExec extends LimitExec {
  name(): string {
    return "LocalLimit";
  }
}

export class GlobalLimitExec extends LimitExec {
  name(): string {
    return "GlobalLimit";
  }
}
