// Cost-based join order for each region of inner and cross joins.
//
// A region is a maximal tree of inner/cross joins; its leaves are the other nodes
// under it (scans with their filters, left joins, aggregates, ...), its predicates all
// the conjuncts of its join conditions. Dynamic programming over connected subsets of
// leaves (bushy trees allowed; cross products only between parts no condition
// connects) finds the cheapest tree under
//
//   cost(join) = cost(left) + cost(right) + BUILD * rows(right) + rows(left) + OUTPUT * rows(out)
//
// with rows from cardinality.ts. The right input is the hash join's build side, so
// this also picks build sides. The written order is kept unless the best one is at
// least 10% cheaper. A Project on top restores the root's column order if it changed.
// Regions of more than MAX_LEAVES leaves are left alone.

import { conjuncts, refIds, type Expr } from "../../core/expr.ts";
import { andAll } from "../../core/functions/index.ts";
import { Join, Project, type PlanNode } from "../plan.ts";
import { Estimator, conjunctSelectivity, type Est } from "./cardinality.ts";
import { filterIf } from "./filter-pushdown.ts";
import { mapChildren, type Rule } from "./rule.ts";

type Shape = number | readonly [Shape, Shape];

const BUILD_COST = 1.2;
const OUTPUT_COST = 0.3;
const MAX_LEAVES = 10;
const MIN_GAIN = 0.9;

const popcount = (m: number) => {
  let c = 0;
  for (; m; m &= m - 1) c++;
  return c;
};

export class JoinReorder implements Rule {
  readonly name = "join_reorder";
  readonly doc = "Picks the cheapest order and build sides of inner joins from estimated row counts (statistics and sketches).";
  private estimator = new Estimator();

  apply(plan: PlanNode) {
    this.estimator = new Estimator();
    const out = this.visit(plan);
    if (out === plan) return { plan, changed: false };
    const before = plan.schema.map((f) => f.id).join(","), after = out.schema.map((f) => f.id).join(",");
    const fixed = before === after ? out : new Project(out, plan.schema.map((f) => f.ref()), plan.schema);
    return { plan: fixed, changed: true };
  }

  private visit(node: PlanNode): PlanNode {
    if (node instanceof Join && node.how !== "left") return this.region(node);
    return mapChildren(node, (c) => this.visit(c));
  }

  private region(root: Join): PlanNode {
    let leaves: PlanNode[] = [];
    const conds: Expr[] = [];
    const flatten = (n: PlanNode): Shape => {
      if (n instanceof Join && n.how !== "left") {
        if (n.condition) conds.push(...conjuncts(n.condition));
        return [flatten(n.left), flatten(n.right)];
      }
      leaves.push(n);
      return leaves.length - 1;
    };
    const shape = flatten(root);
    const visited = leaves.map((l) => this.visit(l));
    const changedLeaves = visited.some((l, i) => l !== leaves[i]);
    leaves = visited;
    const owner = new Map<number, number>();
    leaves.forEach((leaf, i) => leaf.schema.forEach((f) => owner.set(f.id, i)));
    const masks: (number | null)[] = [];
    for (const c of conds) {
      const ids = [...refIds(c)];
      if (!ids.length || ids.some((i) => !owner.has(i))) {
        return changedLeaves ? build(shape, leaves, conds, conds.map(() => null)) : root;
      }
      masks.push([...new Set(ids.map((i) => owner.get(i)!))].reduce((m, i) => m | (1 << i), 0));
    }
    if (leaves.length > MAX_LEAVES) return changedLeaves ? build(shape, leaves, conds, masks) : root;
    const model = new JoinCost(leaves, conds, masks as number[], owner, this.estimator);
    const written = model.cost(shape);
    const best = model.best();
    if (!best || sameShape(best.shape, shape) || best.cost >= written * MIN_GAIN) {
      return changedLeaves ? build(shape, leaves, conds, masks) : root;
    }
    return build(best.shape, leaves, conds, masks);
  }
}

function sameShape(a: Shape, b: Shape): boolean {
  if (typeof a === "number" || typeof b === "number") return a === b;
  return sameShape(a[0], b[0]) && sameShape(a[1], b[1]);
}

/** The join tree for `shape`, each conjunct at the lowest join holding all its leaves
 * (a single-leaf conjunct becomes a Filter on that leaf). */
function build(shape: Shape, leaves: readonly PlanNode[], conds: readonly Expr[], masks: readonly (number | null)[]): PlanNode {
  const pending = new Map(conds.map((c, i) => [i, [c, masks[i]!] as const]));
  const take = (mask: number, insideOnly: boolean): Expr[] => {
    const out: Expr[] = [];
    for (const [k, [c, m]] of pending) {
      if (m !== null && (m & ~mask) === 0 && (!insideOnly || m === mask)) {
        out.push(c);
        pending.delete(k);
      }
    }
    return out;
  };
  const visit = (s: Shape): [PlanNode, number] => {
    if (typeof s === "number") return [filterIf(leaves[s]!, take(1 << s, true)), 1 << s];
    const [l, lm] = visit(s[0]), [r, rm] = visit(s[1]);
    const here = take(lm | rm, false);
    return [new Join(l, r, here.length ? "inner" : "cross", andAll(here)), lm | rm];
  };
  const [node] = visit(shape);
  return filterIf(node, [...pending.values()].map(([c]) => c));
}

class JoinCost {
  private readonly n: number;
  private readonly ests: Est[];
  private readonly preds: [mask: number, sel: number][];
  private readonly memo = new Map<number, number>();

  constructor(leaves: readonly PlanNode[], conds: readonly Expr[], masks: readonly number[], owner: ReadonlyMap<number, number>, estimator: Estimator) {
    this.n = leaves.length;
    this.ests = leaves.map((l) => estimator.estimate(l));
    const byId = new Map<number, Est>();
    for (const [id, i] of owner) byId.set(id, this.ests[i]!);
    this.preds = conds.map((c, i) => [masks[i]!, conjunctSelectivity(c, byId)]);
  }

  rows(mask: number): number {
    let r = this.memo.get(mask);
    if (r === undefined) {
      r = 1;
      for (let i = 0; i < this.n; i++) if ((mask >> i) & 1) r *= this.ests[i]!.rows;
      for (const [m, s] of this.preds) if ((m & ~mask) === 0 && (m & (m - 1))) r *= s;
      this.memo.set(mask, r);
    }
    return r;
  }

  private joins(a: number, b: number): boolean {
    return this.preds.some(([m]) => (m & a) !== 0 && (m & b) !== 0 && (m & ~(a | b)) === 0);
  }

  private joinCost(a: number, b: number): number {
    return BUILD_COST * this.rows(b) + this.rows(a) + OUTPUT_COST * this.rows(a | b);
  }

  cost(shape: Shape): number {
    const walk = (s: Shape): [number, number] => {
      if (typeof s === "number") return [0, 1 << s];
      const [ca, a] = walk(s[0]), [cb, b] = walk(s[1]);
      return [ca + cb + this.joinCost(a, b), a | b];
    };
    return walk(shape)[0];
  }

  best(): { cost: number; shape: Shape } | null {
    const full = (1 << this.n) - 1;
    const best = new Map<number, { cost: number; shape: Shape }>();
    for (let i = 0; i < this.n; i++) best.set(1 << i, { cost: 0, shape: i });
    for (let mask = 1; mask <= full; mask++) {
      if (best.has(mask) || (mask & (mask - 1)) === 0) continue;
      let found: { cost: number; shape: Shape } | null = null;
      for (let sub = (mask - 1) & mask; sub; sub = (sub - 1) & mask) {
        const other = mask ^ sub;
        const a = best.get(sub), b = best.get(other);
        if (a && b && this.joins(sub, other)) {
          const c = a.cost + b.cost + this.joinCost(sub, other);
          if (!found || c < found.cost) found = { cost: c, shape: [a.shape, b.shape] };
        }
      }
      if (found) best.set(mask, found);
    }
    if (best.has(full)) return best.get(full)!;
    // Disconnected: join the connected parts with cross joins, the largest as probe.
    const parts: number[] = [];
    let remaining = full;
    for (const mask of [...best.keys()].sort((a, b) => popcount(b) - popcount(a))) {
      if ((mask & remaining) === mask) {
        parts.push(mask);
        remaining &= ~mask;
      }
    }
    parts.sort((a, b) => this.rows(b) - this.rows(a));
    let { cost, shape } = best.get(parts[0]!)!;
    let acc = parts[0]!;
    for (const m of parts.slice(1)) {
      cost += best.get(m)!.cost + this.joinCost(acc, m);
      shape = [shape, best.get(m)!.shape];
      acc |= m;
    }
    return { cost, shape };
  }
}
