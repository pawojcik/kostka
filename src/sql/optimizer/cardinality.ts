// Row-count and distinct-count estimates for plan nodes, from the catalog's per-file
// statistics of the files a Scan still reads: row counts, null counts, min/max and
// HyperLogLog sketches. The join reorder rule and the physical planner's broadcast
// choice use them. They are estimates for choosing plans: errors of 2x are normal.
//
// - Scan: rows = the files' row counts. A column's distinct count is the union of the
//   files' sketches (exact for one file); each file's (rows, min, max) is one
//   histogram bucket for range predicates.
// - Filter: rows x selectivity. `c = L` -> 1/distinct; `c <> L` the rest; `c < L` etc.
//   the buckets' fraction below/above L (values spread evenly inside a file);
//   IS NULL the null fraction; AND multiplies, OR adds minus the overlap, NOT
//   complements; anything else 1/3.
// - Join: rows(l) x rows(r) x the product over `a = b` pairs of the overlap-aware
//   1 / max(distinct a, distinct b); other conjuncts 1/3. A left join keeps >= rows(l).
// - Aggregate: min(rows, product of the keys' distinct counts); 1 without keys.
// - Limit / TopN: min(n, rows).

import { Alias, Call, Cast, ColumnRef, Literal, conjuncts, isCall, type Expr } from "../../core/expr.ts";
import { DAY_US, type ColumnType, type Value } from "../../core/types.ts";
import { HyperLogLog } from "../../storage/sketch.ts";
import { Aggregate, Filter, Join, Limit, Project, Scan, Sort, TopN, type PlanNode } from "../plan.ts";

export const UNKNOWN = 1 / 3;
const FLIP: Record<string, string> = { "<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "<>": "<>" };

type Bucket = readonly [rows: number, lo: number, hi: number];

/** A position on one axis for range estimates: numbers as they are, dates as micros. */
function position(v: Value, t: string): number | null {
  if (v === null || typeof v === "boolean" || typeof v === "string") return null;
  return t === "date" ? v * DAY_US : v;
}

export class Col {
  private ndvValue: number | null;
  private ndvFn: (() => number) | null;
  readonly nullFrac: number;
  readonly buckets: readonly Bucket[];
  constructor(ndv: number | null = null, nullFrac = 0, buckets: readonly Bucket[] = [], ndvFn: (() => number) | null = null) {
    this.ndvValue = ndv;
    this.ndvFn = ndvFn;
    this.nullFrac = nullFrac;
    this.buckets = buckets;
  }
  get ndv(): number | null {
    if (this.ndvValue === null && this.ndvFn) {
      this.ndvValue = this.ndvFn();
      this.ndvFn = null;
    }
    return this.ndvValue;
  }
  capped(rows: number): Col {
    const n = this.ndv;
    return new Col(n === null ? null : Math.max(1, Math.min(n, rows)), this.nullFrac, this.buckets);
  }
}

export class Est {
  readonly rows: number;
  readonly cols: ReadonlyMap<number, Col>;
  constructor(rows: number, cols: ReadonlyMap<number, Col>) {
    this.rows = Math.max(0, rows);
    this.cols = cols;
  }
  ndv(id: number): number {
    const n = this.cols.get(id)?.ndv ?? null;
    return Math.max(1, n === null ? this.rows : Math.min(this.rows, n));
  }
  scaled(rows: number, exact: ReadonlySet<number> = new Set()): Est {
    const cols = new Map<number, Col>();
    for (const [i, c] of this.cols) cols.set(i, exact.has(i) ? new Col(1, 0, c.buckets) : c.capped(rows));
    return new Est(rows, cols);
  }
}

export class Estimator {
  private readonly memo = new WeakMap<PlanNode, Est>();

  estimate(node: PlanNode): Est {
    let e = this.memo.get(node);
    if (!e) {
      e = this.compute(node);
      this.memo.set(node, e);
    }
    return e;
  }

  rows(node: PlanNode): number {
    return this.estimate(node).rows;
  }

  private compute(node: PlanNode): Est {
    if (node instanceof Scan) return scanEstimate(node);
    if (node instanceof Filter) {
      const child = this.estimate(node.child);
      return child.scaled(child.rows * selectivity(node.condition, child), pinned(node.condition));
    }
    if (node instanceof Project) {
      const child = this.estimate(node.child);
      const cols = new Map<number, Col>();
      node.schema.forEach((f, i) => {
        const e = unwrap(node.exprs[i]!);
        cols.set(f.id, e instanceof ColumnRef ? child.cols.get(e.id) ?? new Col() : new Col());
      });
      return new Est(child.rows, cols);
    }
    if (node instanceof Aggregate) {
      const child = this.estimate(node.child);
      if (!node.keys.length) return new Est(1, new Map());
      let groups = 1;
      const cols = new Map<number, Col>();
      node.keys.forEach((k, i) => {
        const e = unwrap(k);
        let n: number;
        if (e instanceof ColumnRef) {
          n = child.ndv(e.id);
          cols.set(node.schema[i]!.id, (child.cols.get(e.id) ?? new Col()).capped(n));
        } else n = child.rows / 10; // a computed key: assume some repetition
        groups *= Math.max(1, n);
      });
      const rows = Math.min(child.rows, groups);
      return new Est(rows, cols).scaled(rows);
    }
    if (node instanceof Sort) return this.estimate(node.child);
    if (node instanceof Limit || node instanceof TopN) {
      const child = this.estimate(node.child);
      return child.scaled(Math.min(child.rows, node.n));
    }
    if (node instanceof Join) {
      const l = this.estimate(node.left), r = this.estimate(node.right);
      let rows = l.rows * r.rows * joinSelectivity(node.condition, l, r);
      if (node.how === "left") rows = Math.max(rows, l.rows);
      return new Est(rows, new Map([...l.cols, ...r.cols])).scaled(rows);
    }
    return new Est(1000, new Map());
  }
}

const unwrap = (e: Expr): Expr => {
  let x = e instanceof Alias ? e.child : e;
  while (x instanceof Cast && x.implicit) x = x.child;
  return x;
};

function scanEstimate(scan: Scan): Est {
  const files = scan.info.files;
  const rows = files.reduce((s, f) => s + f.rowCount, 0);
  const cols = new Map<number, Col>();
  scan.schema.forEach((field, i) => {
    const name = scan.info.columns[i]!, type = scan.info.types[i]!;
    const stats = files.map((f) => [f, f.stats.get(name)] as const);
    const nulls = stats.reduce((s, [, st]) => s + (st?.nullCount ?? 0), 0);
    const buckets: Bucket[] = [];
    for (const [f, st] of stats) {
      if (!st) continue;
      const lo = position(st.min, type), hi = position(st.max, type);
      if (lo !== null && hi !== null) buckets.push([f.rowCount - st.nullCount, lo, hi]);
    }
    cols.set(field.id, new Col(null, rows ? nulls / rows : 0, buckets, ndvFn(type, stats.map(([, s]) => s), rows - nulls)));
  });
  return new Est(rows, cols);
}

function ndvFn(type: ColumnType, stats: readonly (import("../../catalog/meta.ts").ColumnStats | undefined)[], present: number): (() => number) | null {
  if (type === "bool") return () => Math.min(2, Math.max(1, present));
  if (!stats.length || stats.some((s) => !s || s.distinct === null)) return null;
  if (stats.length === 1) {
    const exact = stats[0]!.distinct!;
    return () => exact;
  }
  return () => {
    const sketches = stats.flatMap((s) => (s!.sketch ? [s!.sketch] : []));
    const unsketched = stats.reduce((n, s) => n + (s!.sketch ? 0 : s!.distinct!), 0);
    const union = sketches.length ? HyperLogLog.union(sketches).estimate() : 0;
    return Math.max(1, Math.min(present, union + unsketched));
  };
}

/** Column ids a filter fixes to one value (`c = L` conjuncts). */
function pinned(cond: Expr): Set<number> {
  const out = new Set<number>();
  for (const c of conjuncts(cond)) {
    if (!isCall(c, "=")) continue;
    const [a, b] = c.args.map(unwrap) as [Expr, Expr];
    if (a instanceof ColumnRef && b instanceof Literal) out.add(a.id);
    if (b instanceof ColumnRef && a instanceof Literal) out.add(b.id);
  }
  return out;
}

export function selectivity(e: Expr, est: Est): number {
  return Math.min(1, Math.max(0, sel(e, est)));
}

function sel(e: Expr, est: Est): number {
  if (isCall(e, "and")) return sel(e.args[0]!, est) * sel(e.args[1]!, est);
  if (isCall(e, "or")) {
    const a = sel(e.args[0]!, est), b = sel(e.args[1]!, est);
    return a + b - a * b;
  }
  if (isCall(e, "not")) return 1 - sel(e.args[0]!, est);
  if (isCall(e, "is_null") || isCall(e, "is_not_null")) {
    const x = unwrap(e.args[0]!);
    const frac = x instanceof ColumnRef ? est.cols.get(x.id)?.nullFrac ?? 0.1 : 0.1;
    return e.fn.name === "is_null" ? frac : 1 - frac;
  }
  if (isCall(e, "in")) {
    const [x, ...list] = e.args.map(unwrap);
    if (!(x instanceof ColumnRef)) return UNKNOWN;
    const each = list.map((v) => (v instanceof Literal ? compare(x.id, "=", v.value, est, v.type) : UNKNOWN));
    return Math.min(1, each.reduce((a, b) => a + b, 0));
  }
  if (e instanceof ColumnRef) return 0.5;
  if (e instanceof Literal) return e.value === true ? 1 : 0;
  if (e instanceof Call && e.fn.name in FLIP) {
    const [a, b] = e.args.map(unwrap) as [Expr, Expr];
    if (a instanceof ColumnRef && b instanceof Literal) return compare(a.id, e.fn.name, b.value, est, b.type);
    if (b instanceof ColumnRef && a instanceof Literal) return compare(b.id, FLIP[e.fn.name]!, a.value, est, a.type);
    if (e.fn.name === "=" && a instanceof ColumnRef && b instanceof ColumnRef) return 1 / Math.max(est.ndv(a.id), est.ndv(b.id));
    return e.fn.name === "=" ? 0.1 : UNKNOWN;
  }
  return UNKNOWN;
}

function compare(id: number, op: string, value: Value, est: Est, type: string): number {
  if (value === null) return 0;
  const c = est.cols.get(id);
  const present = c ? 1 - c.nullFrac : 1;
  if (op === "=" || op === "<>") {
    const eq = 1 / est.ndv(id);
    return present * (op === "=" ? eq : 1 - eq);
  }
  const x = position(value, type);
  if (!c || x === null || !c.buckets.length) return present * UNKNOWN;
  const total = c.buckets.reduce((s, [n]) => s + n, 0);
  if (!total) return 0;
  let below = 0;
  for (const [n, lo, hi] of c.buckets) {
    const frac = hi <= lo ? (x > lo ? 1 : x === lo ? 0.5 : 0) : Math.min(1, Math.max(0, (x - lo) / (hi - lo)));
    below += n * frac;
  }
  const frac = below / total;
  return present * (op === "<" || op === "<=" ? frac : 1 - frac);
}

export function conjunctSelectivity(c: Expr, byId: ReadonlyMap<number, Est>): number {
  if (isCall(c, "=")) {
    const [a, b] = c.args.map(unwrap) as [Expr, Expr];
    if (a instanceof ColumnRef && b instanceof ColumnRef && byId.has(a.id) && byId.has(b.id)) {
      return equiSelectivity(a.id, byId.get(a.id)!, b.id, byId.get(b.id)!);
    }
  }
  return UNKNOWN;
}

/** `a = b`: 1 / max(distinct a, distinct b), over the overlap of both columns' ranges. */
export function equiSelectivity(a: number, ea: Est, b: number, eb: Est): number {
  let fa = 1, fb = 1;
  const ca = ea.cols.get(a), cb = eb.cols.get(b);
  if (ca && cb && ca.buckets.length && cb.buckets.length) {
    const lo = Math.max(Math.min(...ca.buckets.map((x) => x[1])), Math.min(...cb.buckets.map((x) => x[1])));
    const hi = Math.min(Math.max(...ca.buckets.map((x) => x[2])), Math.max(...cb.buckets.map((x) => x[2])));
    if (lo > hi) return 0;
    fa = inside(ca.buckets, lo, hi);
    fb = inside(cb.buckets, lo, hi);
    if (!fa || !fb) return 0;
  }
  return (fa * fb) / Math.max(ea.ndv(a) * fa, eb.ndv(b) * fb, 1);
}

function inside(buckets: readonly Bucket[], lo: number, hi: number): number {
  let total = 0, n = 0;
  for (const [rows, x, y] of buckets) {
    total += rows;
    if (y <= x) n += lo <= x && x <= hi ? rows : 0;
    else n += (rows * Math.max(0, Math.min(hi, y) - Math.max(lo, x))) / (y - x);
  }
  return total ? n / total : 0;
}

export function joinSelectivity(cond: Expr | null, l: Est, r: Est): number {
  if (!cond) return 1;
  const byId = new Map<number, Est>();
  for (const i of l.cols.keys()) byId.set(i, l);
  for (const i of r.cols.keys()) byId.set(i, r);
  return conjuncts(cond).reduce((acc, c) => acc * conjunctSelectivity(c, byId), 1);
}
