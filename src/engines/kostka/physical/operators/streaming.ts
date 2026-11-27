// Streaming operators: Filter, Project, Limit and the runtime-filter row filter.

import { compileExpr, compilePredicate, type Evaluator } from "../../../../core/compile.ts";
import type { Expr, Field } from "../../../../core/expr.ts";
import type { Row } from "../../../../core/types.ts";
import { checkColumns, type Batch } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST, exprCost } from "../../../../cluster/cost.ts";
import { Operator, emit, type OperatorInstance } from "../operator.ts";
import type { RuntimeFilter } from "./runtime-filter.ts";

export class FilterOp extends Operator {
  readonly condition: Expr;
  private readonly test: (row: Row) => boolean;
  private readonly cost: number;
  constructor(condition: Expr, columns: readonly Field[]) {
    super(columns, columns);
    this.condition = condition;
    this.test = compilePredicate(condition, columns);
    this.cost = exprCost(condition) + COST.row;
  }
  label(): string {
    return "Filter";
  }
  instance(me: Charged, next: OperatorInstance): OperatorInstance {
    return {
      push: (ctx, batch) => {
        checkColumns(batch, this.input, "Filter");
        me.metrics.tookIn(batch);
        ctx.cpu(batch.rows.length * this.cost, me);
        return emit(me, this, ctx, next, batch.rows.filter(this.test));
      },
      finish: (ctx) => next.finish(ctx),
      state: () => ({ kept: me.metrics.rowsOut, dropped: me.metrics.rowsIn - me.metrics.rowsOut }),
    };
  }
  override describe() {
    return { condition: this.condition.display() };
  }
}

export class ProjectOp extends Operator {
  readonly exprs: readonly Expr[];
  private readonly evals: Evaluator[];
  private readonly cost: number;
  constructor(exprs: readonly Expr[], input: readonly Field[], output: readonly Field[]) {
    super(input, output);
    this.exprs = exprs;
    this.evals = exprs.map((e) => compileExpr(e, input));
    this.cost = exprs.reduce<number>((s, e) => s + exprCost(e), COST.row);
  }
  label(): string {
    return "Project";
  }
  instance(me: Charged, next: OperatorInstance): OperatorInstance {
    const evals = this.evals;
    return {
      push: (ctx, batch) => {
        checkColumns(batch, this.input, "Project");
        me.metrics.tookIn(batch);
        ctx.cpu(batch.rows.length * this.cost, me);
        const out = new Array<Row>(batch.rows.length);
        for (let i = 0; i < out.length; i++) {
          const row = batch.rows[i]!;
          const r = new Array(evals.length);
          for (let j = 0; j < evals.length; j++) r[j] = evals[j]!(row);
          out[i] = r;
        }
        return emit(me, this, ctx, next, out);
      },
      finish: (ctx) => next.finish(ctx),
    };
  }
  override describe() {
    return { exprs: this.exprs.map((e) => e.display()) };
  }
}

/** Counts the rows it lets through in its split; once it is full, the split stops
 * reading. In a pipeline with several splits it is the local half of a two-phase
 * limit (the planner adds a gather and a final Limit that reads it as one split).
 * Without an ORDER BY below it, which rows it keeps depends on the schedule. */
export class LimitOp extends Operator {
  readonly n: number;
  constructor(n: number, columns: readonly Field[]) {
    super(columns, columns);
    this.n = n;
  }
  label(): string {
    return `Limit(${this.n})`;
  }
  instance(me: Charged, next: OperatorInstance): OperatorInstance {
    let remaining = this.n;
    return {
      push: (ctx, batch: Batch) => {
        checkColumns(batch, this.input, "Limit");
        me.metrics.tookIn(batch);
        const take = batch.rows.slice(0, Math.max(0, remaining));
        remaining -= take.length;
        ctx.cpu(take.length * COST.row, me);
        const more = emit(me, this, ctx, next, take);
        return more && remaining > 0;
      },
      finish: (ctx) => next.finish(ctx),
      state: () => ({ remaining: Math.max(0, remaining), reached: remaining <= 0 }),
    };
  }
  override describe() {
    return { limit: this.n };
  }
}

/** Drops probe-side rows whose join key is not among a later join's build keys,
 * right after the scan, so the probes in between see fewer rows. The keys come from
 * the coordinator with the pipeline's splits. */
export class RuntimeFilterOp extends Operator {
  readonly filters: [slot: number, filter: RuntimeFilter][] = [];
  constructor(columns: readonly Field[]) {
    super(columns, columns);
  }
  label(): string {
    return `RuntimeFilter(${this.filters.length} key(s))`;
  }
  instance(me: Charged, next: OperatorInstance): OperatorInstance {
    return {
      push: (ctx: ExecContext, batch) => {
        checkColumns(batch, this.input, "RuntimeFilter");
        me.metrics.tookIn(batch);
        let rows = batch.rows;
        for (const [slot, rf] of this.filters) {
          const keys = ctx.filterKeys(rf.id);
          if (!keys) continue;
          ctx.cpu(rows.length * COST.hashProbe, me);
          rows = rows.filter((r) => r[slot] !== null && keys.has(r[slot]!));
        }
        return emit(me, this, ctx, next, rows);
      },
      finish: (ctx) => next.finish(ctx),
      state: () => ({ kept: me.metrics.rowsOut, dropped: me.metrics.rowsIn - me.metrics.rowsOut }),
    };
  }
  override describe() {
    return { filters: this.filters.map(([, rf]) => rf.label) };
  }
}
