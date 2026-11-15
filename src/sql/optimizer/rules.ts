// The default rule list, and the two small rules.

import { refIds, refIdsOf } from "../../core/expr.ts";
import { internal } from "../../core/errors.ts";
import { Aggregate, Filter, Join, Limit, Project, Scan, Sort, TopN, idsOf, type PlanNode } from "../plan.ts";
import { FilePruning } from "./file-pruning.ts";
import { FilterPushdown } from "./filter-pushdown.ts";
import { JoinReorder } from "./join-reorder.ts";
import { rewritePlan, type Rule } from "./rule.ts";

/** Makes every Scan read only the columns something above it uses. The root keeps
 * its output; `count(*)` alone reads no columns at all. */
export class ProjectionPushdown implements Rule {
  readonly name = "projection_pushdown";
  readonly doc = "Makes each scan read only the columns the query uses.";

  apply(plan: PlanNode) {
    const out = this.prune(plan, idsOf(plan));
    if (out.schema.map((f) => f.id).join() !== plan.schema.map((f) => f.id).join()) {
      throw internal("projection pushdown changed the output");
    }
    return { plan: out, changed: out !== plan };
  }

  private prune(node: PlanNode, needed: Set<number>): PlanNode {
    const union = (...sets: Iterable<number>[]) => {
      const out = new Set<number>();
      for (const s of sets) for (const x of s) out.add(x);
      return out;
    };
    let kids: PlanNode[];
    if (node instanceof Scan) {
      const keep = node.schema.flatMap((f, i) => (needed.has(f.id) ? [i] : []));
      return keep.length === node.schema.length ? node : node.derive({ keep });
    } else if (node instanceof Filter) {
      kids = [this.prune(node.child, union(needed, refIds(node.condition)))];
    } else if (node instanceof Sort || node instanceof TopN) {
      kids = [this.prune(node.child, union(needed, refIdsOf(node.keys.map((k) => k.expr))))];
    } else if (node instanceof Limit) {
      kids = [this.prune(node.child, needed)];
    } else if (node instanceof Project) {
      // Outputs nothing above uses are dropped too (the root needs all of its own).
      const keep = node.schema.flatMap((f, i) => (needed.has(f.id) ? [i] : []));
      const exprs = keep.map((i) => node.exprs[i]!);
      const child = this.prune(node.child, refIdsOf(exprs));
      if (keep.length === node.schema.length) return child === node.child ? node : node.withChildren([child]);
      return new Project(child, exprs, keep.map((i) => node.schema[i]!));
    } else if (node instanceof Aggregate) {
      kids = [this.prune(node.child, union(refIdsOf(node.keys), refIdsOf(node.aggs)))];
    } else if (node instanceof Join) {
      const used = union(needed, node.condition ? refIds(node.condition) : []);
      const l = idsOf(node.left), r = idsOf(node.right);
      kids = [this.prune(node.left, new Set([...used].filter((i) => l.has(i)))), this.prune(node.right, new Set([...used].filter((i) => r.has(i))))];
    } else {
      return node;
    }
    return kids.every((k, i) => k === node.children()[i]) ? node : node.withChildren(kids);
  }
}

/** Limit(Sort(x)) -> TopN(x): the sort keeps only the best n rows. */
export class TopNFusion implements Rule {
  readonly name = "topn";
  readonly doc = "Fuses ORDER BY with LIMIT into a top-n that keeps only the best n rows.";

  apply(plan: PlanNode) {
    return rewritePlan(plan, (node) => (node instanceof Limit && node.child instanceof Sort
      ? new TopN(node.child.child, node.child.keys, node.n) : null));
  }
}

export const DEFAULT_RULES: readonly Rule[] = [
  new FilterPushdown(), new FilePruning(), new JoinReorder(), new ProjectionPushdown(), new TopNFusion(),
];

export { FilePruning, FilterPushdown, JoinReorder };
