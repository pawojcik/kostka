// Moves filters as close to the scans as is safe.
//
// One pass moves every filter at most one step and the optimizer repeats it until
// nothing moves:
//   Filter(Filter)            -> one Filter, conditions and-ed
//   Filter(Sort)              -> Sort(Filter)
//   Filter(Project)           -> Project(Filter), computed columns substituted
//   Filter(Aggregate)         -> conjuncts on group keys only go below (never for a
//                                global aggregate)
//   Filter(Join inner/cross)  -> left-only / right-only conjuncts go to that side, ones
//                                on both sides join the condition (cross becomes inner),
//                                constants go left
//   Filter(Join left)         -> left-only conjuncts go left; the rest stays above
//   Filter(Limit / TopN / Scan) unchanged

import { Alias, ColumnRef, conjuncts, refIds, subsetOf, transform, type Expr, type Field } from "../../core/expr.ts";
import { andAll } from "../../core/functions/index.ts";
import { Aggregate, Filter, Join, Project, Sort, idsOf, type PlanNode } from "../plan.ts";
import { mapChildren, type Rule } from "./rule.ts";

export function filterIf(child: PlanNode, parts: readonly Expr[]): PlanNode {
  const cond = andAll(parts);
  return cond ? new Filter(child, cond) : child;
}

/** Replaces references to the given ids with their defining expressions. */
export function substitute(e: Expr, defs: ReadonlyMap<number, Expr>): Expr {
  return transform(e, (x) => (x instanceof ColumnRef && defs.has(x.id) ? defs.get(x.id)! : x));
}

/** id -> defining expression, for the computed outputs of a Project or Aggregate. */
export function definitions(exprs: readonly Expr[], schema: readonly Field[]): Map<number, Expr> {
  const out = new Map<number, Expr>();
  exprs.forEach((e, i) => {
    if (!(e instanceof ColumnRef) || e.id !== schema[i]!.id) out.set(schema[i]!.id, e instanceof Alias ? e.child : e);
  });
  return out;
}

export class FilterPushdown implements Rule {
  readonly name = "filter_pushdown";
  readonly doc = "Moves filter conditions down, below projections, sorts, group keys and into joins, so fewer rows flow.";

  apply(plan: PlanNode) {
    let changed = false;
    const visit = (node: PlanNode): PlanNode => {
      if (node instanceof Filter) {
        const pushed = this.push(node);
        if (pushed) {
          changed = true;
          node = pushed;
        }
      }
      return mapChildren(node, visit);
    };
    const out = visit(plan);
    return { plan: out, changed };
  }

  private push(f: Filter): PlanNode | null {
    const child = f.child;
    const cond = f.condition;
    if (child instanceof Filter) return new Filter(child.child, andAll([child.condition, cond])!);
    if (child instanceof Sort) return child.withChildren([new Filter(child.child, cond)]);
    if (child instanceof Project) {
      const pushed = substitute(cond, definitions(child.exprs, child.schema));
      return child.withChildren([new Filter(child.child, pushed)]);
    }
    if (child instanceof Aggregate && child.keys.length) {
      const keyFields = child.schema.slice(0, child.keys.length);
      const keyIds = new Set(keyFields.map((x) => x.id));
      const down: Expr[] = [], keep: Expr[] = [];
      for (const c of conjuncts(cond)) {
        const ids = refIds(c);
        (ids.size && subsetOf(ids, keyIds) ? down : keep).push(c);
      }
      if (!down.length) return null;
      const defs = definitions(child.keys, keyFields);
      const below = new Filter(child.child, andAll(down.map((c) => substitute(c, defs)))!);
      return filterIf(child.withChildren([below]), keep);
    }
    if (child instanceof Join) return this.intoJoin(f, child);
    return null;
  }

  private intoJoin(f: Filter, join: Join): PlanNode | null {
    const leftIds = idsOf(join.left), rightIds = idsOf(join.right);
    const toLeft: Expr[] = [], toRight: Expr[] = [], toJoin: Expr[] = [], keep: Expr[] = [];
    for (const c of conjuncts(f.condition)) {
      const ids = refIds(c);
      if (join.how === "left") (ids.size && subsetOf(ids, leftIds) ? toLeft : keep).push(c);
      else if (!ids.size || subsetOf(ids, leftIds)) toLeft.push(c);
      else if (subsetOf(ids, rightIds)) toRight.push(c);
      else toJoin.push(c);
    }
    if (!toLeft.length && !toRight.length && !toJoin.length) return null;
    const how = join.how === "cross" && toJoin.length ? "inner" : join.how;
    const next = new Join(filterIf(join.left, toLeft), filterIf(join.right, toRight), how, andAll([join.condition, ...toJoin]));
    return filterIf(next, keep);
  }
}
