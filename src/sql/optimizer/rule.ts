// The rule-based optimizer. A rule rewrites a plan into an equivalent one:
// the same output columns (ids, names and order) and the same rows, in the same order
// only when the plan ends in a sort. Errors may appear or disappear (an expression may
// be evaluated elsewhere, or not at all).
//
// `optimize` runs each rule to a fixpoint, in order. A rule never mutates its input.
// To add one, implement `Rule` and put it in DEFAULT_RULES (rules.ts) where it should run.

import { internal } from "../../core/errors.ts";
import type { PlanNode } from "../plan.ts";

export interface Rule {
  readonly name: string;
  readonly doc: string;
  apply(plan: PlanNode): { plan: PlanNode; changed: boolean };
}

export interface RuleTiming {
  rule: string;
  iteration: number;
  start: number;
  end: number;
  changed: boolean;
}

export interface OptimizeOptions {
  /** Receives every plan a rule produced, for the workbench's plan steps. */
  trace?: { rule: string; iteration: number; plan: PlanNode }[];
  /** Receives the time of every pass (performance.now() ms). */
  timings?: RuleTiming[];
}

export const MAX_ITERATIONS = 100;

export function optimize(plan: PlanNode, rules: readonly Rule[], options: OptimizeOptions = {}): PlanNode {
  for (const rule of rules) {
    for (let iteration = 1; ; iteration++) {
      if (iteration > MAX_ITERATIONS) throw internal(`rule ${rule.name} did not reach a fixpoint in ${MAX_ITERATIONS} iterations`);
      const start = performance.now();
      const out = rule.apply(plan);
      options.timings?.push({ rule: rule.name, iteration, start, end: performance.now(), changed: out.changed });
      if (!out.changed) break;
      plan = out.plan;
      options.trace?.push({ rule: rule.name, iteration, plan });
    }
  }
  return plan;
}

/** Rebuilds `node` with its children mapped by `fn`, or returns it unchanged. */
export function mapChildren(node: PlanNode, fn: (child: PlanNode) => PlanNode): PlanNode {
  const kids = node.children();
  if (!kids.length) return node;
  const next = kids.map(fn);
  return next.some((k, i) => k !== kids[i]) ? node.withChildren(next) : node;
}

/** A pre-order rewrite: `fn` may replace a node (then it is not descended into further
 * unless `fn` itself recursed) and reports whether it changed anything. */
export function rewritePlan(plan: PlanNode, fn: (node: PlanNode) => PlanNode | null): { plan: PlanNode; changed: boolean } {
  let changed = false;
  const visit = (n: PlanNode): PlanNode => {
    const hit = fn(n);
    if (hit) {
      changed = true;
      return mapChildren(hit, visit);
    }
    return mapChildren(n, visit);
  };
  const out = visit(plan);
  return { plan: out, changed };
}
