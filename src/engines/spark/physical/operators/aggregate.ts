// HashAggregate, in two modes, as Spark plans it:
//
// - partial: each task folds its partition's rows into groups (first-seen order) and
//   gives out one partial row per group: its key and each aggregate's state
//   (core/accumulators.ts). An exchange then sends a group's partial rows to one place.
// - final: each task merges the partial rows of its partition into groups and gives
//   out each group's key and results. A global aggregate (no keys) reads one
//   partition, and over no rows still gives its one row.
//
// The groups must fit in the task's memory share (memoryMB / cores): a hash
// aggregate does not spill here, so one that outgrows it fails the task with
// OutOfMemoryError.

import { AggLayout, type Group } from "../../../../core/accumulators.ts";
import { checkColumns } from "../../../../core/batch.ts";
import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import { COST } from "../../../../core/cost.ts";
import { OutOfMemoryError } from "../../../../core/errors.ts";
import type { AggCall, Expr, Field } from "../../../../core/expr.ts";
import { ENTRY_OVERHEAD, mb, rowsBytes } from "../../../../core/rows.ts";
import { tupleKey, type Row, type Value } from "../../../../core/types.ts";
import type { TaskContext } from "../context.ts";
import { SparkOp, batches, type BatchIter, type Partitioning } from "../plan.ts";

export type AggMode = "partial" | "final";

export class HashAggregateExec extends SparkOp {
  readonly child: SparkOp;
  readonly mode: AggMode;
  readonly keys: readonly Expr[];
  readonly calls: readonly AggCall[];
  readonly layout: AggLayout;
  private readonly keyEvals: Evaluator[];
  private readonly args_: (Evaluator | null)[];

  /** `output`: partial, the keys then the states; final, the keys then the results.
   * A final aggregate's keys are the first columns of its child (the partial rows). */
  constructor(child: SparkOp, mode: AggMode, keys: readonly Expr[], calls: readonly AggCall[], output: readonly Field[]) {
    super(output);
    this.child = child;
    this.mode = mode;
    this.keys = keys;
    this.calls = calls;
    this.layout = new AggLayout(calls);
    this.keyEvals = mode === "partial" ? keys.map((k) => compileExpr(k, child.output)) : [];
    this.args_ = mode === "partial" ? calls.map((c) => (c.arg ? compileExpr(c.arg, child.output) : null)) : [];
  }

  children(): readonly SparkOp[] {
    return [this.child];
  }

  name(): string {
    return "HashAggregate";
  }

  args(): string {
    const fn = this.calls.map((c) => `${this.mode === "partial" ? "partial_" : ""}${c.display()}`);
    return `(keys=[${this.keys.map((k) => k.display()).join(", ")}], functions=[${fn.join(", ")}])`;
  }

  /** A final aggregate's rows are hashed by its key columns, as its input was. */
  partitioning(): Partitioning {
    const p = this.child.partitioning();
    if (this.mode === "partial" || p.kind !== "hash") return p.kind === "hash" ? { kind: "unknown", n: p.n } : p;
    return { kind: "hash", keys: this.output.slice(0, this.keys.length).map((f) => f.ref()), n: p.n };
  }

  execute(ctx: TaskContext, partition: number): BatchIter {
    const input = this.child.execute(ctx, partition);
    const groups = new Map<Value, Group>();
    const k = this.keys.length;
    const impls = this.layout.impls;
    let perGroup = 0, rowsIn = 0;
    for (let b = input.next(); b; b = input.next()) {
      checkColumns(b, this.child.output, "HashAggregate");
      for (const row of b.rows) {
        const kv = this.mode === "partial" ? this.keyEvals.map((e) => e(row)) : row.slice(0, k);
        const ck = tupleKey(kv);
        let g = groups.get(ck);
        if (!g) {
          g = { key: kv, states: this.layout.init() };
          groups.set(ck, g);
        }
        if (this.mode === "partial") this.layout.add(g.states, this.args_, row);
        else this.layout.merge(g.states, row, k);
      }
      rowsIn += b.rows.length;
      ctx.cpu(b.rows.length * (COST.hashProbe + impls.length * COST.aggUpdate), this);
      if (!perGroup && groups.size) {
        const s = [...groups.values()].slice(0, 16).map((g) => this.layout.partial(g));
        perGroup = rowsBytes(s) / s.length;
      }
      const need = Math.round(groups.size * (perGroup + ENTRY_OVERHEAD));
      if (!ctx.reserve(this, `${this.mode} aggregate groups`, need)) {
        throw new OutOfMemoryError(`HashAggregate (${this.mode}) needs ${mb(need)} for ${groups.size.toLocaleString("en")} groups, but a task may hold ${mb(ctx.memoryShare)} (memory per node / cores). Give each node more memory, use fewer cores, or use more shuffle partitions.`);
      }
    }
    if (this.mode === "final" && !k && !groups.size) groups.set(0, { key: [], states: this.layout.init() });
    const out: Row[] = [...groups.values()].map((g) => (this.mode === "partial" ? this.layout.partial(g) : this.layout.result(g)));
    ctx.free(this);
    ctx.note(this, `${this.mode === "partial" ? "folded" : "merged"} ${rowsIn.toLocaleString("en")} row(s) into ${groups.size.toLocaleString("en")} group(s)`);
    return batches(ctx, this, out);
  }
}
