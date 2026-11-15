// A compiled query: SQL -> logical plan (analyzer) -> optimized plan (rules) ->
// physical plan (planner) -> an Execution (a coordinator and workers on a simulated cluster).
// Each compilation phase is timed (`phases`), and each optimizer pass (`timings`).
// The physical plan is immutable; each `start()` runs a fresh one.

import { parse } from "../sql/parser.ts";
import { analyze } from "../sql/analyzer.ts";
import type { PlanNode } from "../sql/plan.ts";
import { optimize, type Rule, type RuleTiming } from "../sql/optimizer/rule.ts";
import { DEFAULT_RULES } from "../sql/optimizer/rules.ts";
import type { CatalogReader } from "../catalog/meta.ts";
import { plan } from "../physical/planner.ts";
import type { PhysicalPlan, PlannerSettings } from "../physical/pipeline.ts";
import type { TableReader } from "../physical/operators/scan.ts";
import { DEFAULT_CLUSTER, type ClusterConfig } from "../execution/cluster.ts";
import type { Execution } from "../execution/runtime.ts";
import { SimExecution } from "../execution/sim/execution.ts";
import type { Field } from "../core/expr.ts";

export interface QueryOptions {
  /** Names of the optimizer rules to run (default: all, in their order). */
  rules?: readonly string[];
  cluster?: Partial<ClusterConfig>;
  planner?: Partial<Omit<PlannerSettings, "workers">>;
}

export const ALL_RULES: readonly Rule[] = DEFAULT_RULES;

/** A compilation phase, in performance.now() ms. */
export interface PhaseTiming {
  phase: "parse" | "analyze" | "optimize" | "physical";
  start: number;
  end: number;
}

export class CompiledQuery {
  readonly sql: string;
  readonly logical: PlanNode;
  readonly optimized: PlanNode;
  readonly trace: { rule: string; iteration: number; plan: PlanNode }[] = [];
  readonly timings: RuleTiming[] = [];
  /** Parse, analyze and optimize, then physical planning (each time start() plans). */
  readonly phases: PhaseTiming[] = [];
  readonly cluster: ClusterConfig;
  readonly planner: Partial<PlannerSettings>;
  private readonly reader: TableReader;

  constructor(sql: string, catalog: CatalogReader, reader: TableReader, o: QueryOptions = {}) {
    this.sql = sql;
    this.reader = reader;
    const timed = <T,>(phase: PhaseTiming["phase"], fn: () => T): T => {
      const start = performance.now();
      try {
        return fn();
      } finally {
        this.phases.push({ phase, start, end: performance.now() });
      }
    };
    const ast = timed("parse", () => parse(sql));
    this.logical = timed("analyze", () => analyze(ast, catalog));
    const rules = o.rules ? ALL_RULES.filter((r) => o.rules!.includes(r.name)) : ALL_RULES;
    this.optimized = timed("optimize", () => optimize(this.logical, rules, { trace: this.trace, timings: this.timings }));
    this.cluster = { ...DEFAULT_CLUSTER, ...o.cluster };
    this.planner = { ...o.planner, workers: this.cluster.workers };
  }

  get columns(): readonly Field[] {
    return this.optimized.schema;
  }

  physical(): PhysicalPlan {
    const start = performance.now();
    const p = plan(this.optimized, this.reader, this.planner);
    const i = this.phases.findIndex((x) => x.phase === "physical");
    if (i >= 0) this.phases.splice(i, 1);
    this.phases.push({ phase: "physical", start, end: performance.now() });
    return p;
  }

  start(): { physical: PhysicalPlan; execution: Execution } {
    const physical = this.physical();
    return { physical, execution: new SimExecution(physical, this.cluster) };
  }
}
