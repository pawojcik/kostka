// A physical plan: pipelines joined by output buffers, independent of who runs
// them. It is immutable: every worker gets the same plan.

import type { Field } from "../core/expr.ts";
import type { OutputBuffer } from "./buffer.ts";
import type { Operator, PhysicalOperator, Sink, Source } from "./operator.ts";
import type { RuntimeFilter } from "./operators/runtime-filter.ts";

export class Pipeline {
  readonly index: number;
  readonly source: Source;
  readonly operators: readonly Operator[];
  readonly sink: Sink;
  /** Pipelines that must be done before this one can start. */
  readonly dependsOn: readonly number[];
  constructor(index: number, source: Source, operators: readonly Operator[], sink: Sink, dependsOn: readonly number[]) {
    this.index = index;
    this.source = source;
    this.operators = operators;
    this.sink = sink;
    this.dependsOn = dependsOn;
    this.chain.forEach((op, i) => (op.id = `P${index}.${i}`));
  }
  get chain(): readonly PhysicalOperator[] {
    return [this.source, ...this.operators, this.sink];
  }
  label(): string {
    return this.chain.map((op) => op.label()).join(" → ");
  }
  toString(): string {
    const needs = this.dependsOn.length ? `  (needs ${this.dependsOn.map((i) => `P${i}`).join(", ")})` : "";
    return `P${this.index}: ${this.label()}${needs}`;
  }
}

/** A plan estimated to produce more rows than this at any node is not run: in a
 * browser, at a few million rows a second, it would take hours or years, and a cross
 * join that big is almost always a mistake. */
export const MAX_PLAN_ROWS = 1e9;

export interface LargestNode {
  readonly rows: number;
  /** The node, as the plan shows it ("Join cross"). */
  readonly node: string;
  /** A join with no condition at all. */
  readonly cross: boolean;
}

export interface PlannerSettings {
  /** The workers the plan is meant for: only used to choose broadcast or shuffle. */
  readonly workers: number;
  /** Partitions of every shuffle (aggregates and shuffle joins); "auto" is 2 per worker,
   * so each worker owns two and an idle one can take over a partition. */
  readonly shufflePartitions: number | "auto";
  readonly joinStrategy: "auto" | "broadcast" | "shuffle";
  readonly runtimeFilters: boolean;
}

export class PhysicalPlan {
  readonly pipelines: readonly Pipeline[];
  readonly buffers: readonly OutputBuffer[];
  readonly filters: readonly RuntimeFilter[];
  readonly columns: readonly Field[];
  readonly settings: PlannerSettings;
  /** The plan node the optimizer estimates the most rows for (null: no estimate). */
  readonly largest: LargestNode | null;
  constructor(pipelines: readonly Pipeline[], buffers: readonly OutputBuffer[], filters: readonly RuntimeFilter[], columns: readonly Field[], settings: PlannerSettings, largest: LargestNode | null = null) {
    this.pipelines = pipelines;
    this.buffers = buffers;
    this.filters = filters;
    this.columns = columns;
    this.settings = settings;
    this.largest = largest;
  }
  /** The pipeline whose sink sends the result. */
  get root(): Pipeline {
    return this.pipelines[this.pipelines.length - 1]!;
  }
  explain(): string {
    return this.pipelines.map(String).join("\n");
  }
}
