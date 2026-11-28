// A compiled query: SQL -> logical plan (analyzer) -> optimized plan (rules) -> an
// Execution on the engine the query names (execution/engine.ts). The engine plans the
// optimized plan in its own way (its "physical" phase) and runs it on the simulated
// cluster. Each compilation phase is timed (`phases`), and each optimizer pass
// (`timings`). The optimized plan is immutable; each `start()` plans and runs afresh.

import { parse } from "../sql/parser.ts";
import { analyze } from "../sql/analyzer.ts";
import type { PlanNode } from "../sql/plan.ts";
import { optimize, type Rule, type RuleTiming } from "../sql/optimizer/rule.ts";
import { DEFAULT_RULES } from "../sql/optimizer/rules.ts";
import type { CatalogReader } from "../catalog/meta.ts";
import type { TableReader } from "../storage/table-reader.ts";
import { clusterConfig, type ClusterConfig, type ClusterInput } from "../cluster/config.ts";
import type { Engine, Execution } from "../execution/engine.ts";
import type { Engines } from "../execution/registry.ts";
import type { Field } from "../core/expr.ts";

export interface QueryOptions {
  /** Names of the optimizer rules to run (default: all, in their order). */
  rules?: readonly string[];
  /** The engine to run on, by name (default: the Database's first engine). */
  engine?: string;
  /** The engine's own settings (kostka: shufflePartitions, joinStrategy,
   * runtimeFilters; Spark-style: broadcastThreshold, shufflePartitions,
   * maxTaskFailures). The engine checks them when the query starts. */
  settings?: Readonly<Record<string, unknown>>;
  /** The same as `settings`, by kostka's old name for them. */
  planner?: Readonly<Record<string, unknown>>;
  cluster?: ClusterInput;
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
  /** Parse, analyze and optimize, then the engine's planning (each time start() plans). */
  readonly phases: PhaseTiming[] = [];
  readonly engine: Engine;
  readonly cluster: ClusterConfig;
  readonly settings: Readonly<Record<string, unknown>>;
  private readonly reader: TableReader;

  constructor(sql: string, catalog: CatalogReader, reader: TableReader, engines: Engines, o: QueryOptions = {}) {
    this.sql = sql;
    this.reader = reader;
    // The engine and the cluster are checked first: a wrong name fails before any work.
    this.engine = engines.get(o.engine);
    this.cluster = clusterConfig(o.cluster ?? {});
    this.settings = { ...o.planner, ...o.settings };
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
    const names = o.rules;
    const rules = names ? ALL_RULES.filter((r) => names.includes(r.name)) : ALL_RULES;
    this.optimized = timed("optimize", () => optimize(this.logical, rules, { trace: this.trace, timings: this.timings }));
  }

  get columns(): readonly Field[] {
    return this.optimized.schema;
  }

  /** Plans the query on its engine and starts an execution; nothing runs yet. */
  start(): Execution {
    const start = performance.now();
    const execution = this.engine.start({ optimized: this.optimized, reader: this.reader, cluster: this.cluster, settings: this.settings });
    const i = this.phases.findIndex((x) => x.phase === "physical");
    if (i >= 0) this.phases.splice(i, 1);
    this.phases.push({ phase: "physical", start, end: performance.now() });
    return execution;
  }
}
