// File pruning: drops the data files of a Scan that a Filter directly above it cannot
// keep any row of, judged from the catalog's per-file column statistics (min/max,
// null counts, bloom filters). The Filter stays: a file that is kept is still filtered
// row by row; skipping only saves work.
//
// `canSkip(expr, columns, file)` is conservative: true only when no row of the file
// can make `expr` TRUE. Understood, with c a column (possibly under an implicit
// widening) and L a literal:
//   c op L, L op c (= <> < <= > >=)   min/max; `=` also the bloom filter of a string column
//   c IN (L, ...)                     every candidate skippable as c = L
//   c IS NULL / c IS NOT NULL         null count
//   c (a bool column)                 max
//   a AND b                           either side skippable
//   a OR b                            both sides skippable

import { Call, Cast, ColumnRef, Literal, conjuncts, isCall, type Expr } from "../../core/expr.ts";
import { castImpl } from "../../core/functions/index.ts";
import { compareValues, type Value } from "../../core/types.ts";
import type { DataFile } from "../../catalog/meta.ts";
import { Filter, Scan, type PlanNode } from "../plan.ts";
import { rewritePlan, type Rule } from "./rule.ts";

const FLIP: Record<string, string> = { "=": "=", "<>": "<>", "<": ">", "<=": ">=", ">": "<", ">=": "<=" };

interface ScanColumn {
  name: string;
  /** Maps a stored value to the type the expression compares in. */
  convert: (v: Value) => Value;
  isString: boolean;
}

/** The scan column `e` reads, seeing through implicit widenings. */
function columnOf(e: Expr, columns: ReadonlyMap<number, string>): ScanColumn | null {
  if (e instanceof ColumnRef) {
    const name = columns.get(e.id);
    return name === undefined ? null : { name, convert: (v) => v, isString: e.type === "string" };
  }
  if (e instanceof Cast && e.implicit && e.child instanceof ColumnRef) {
    const inner = columnOf(e.child, columns);
    const impl = castImpl(e.child.type, e.type);
    if (!inner || !impl) return null;
    return { ...inner, convert: (v) => (v === null ? null : impl(v)), isString: false };
  }
  return null;
}

export function canSkip(e: Expr, columns: ReadonlyMap<number, string>, file: DataFile): boolean {
  if (isCall(e, "and")) return canSkip(e.args[0]!, columns, file) || canSkip(e.args[1]!, columns, file);
  if (isCall(e, "or")) return canSkip(e.args[0]!, columns, file) && canSkip(e.args[1]!, columns, file);
  if (e instanceof Call && e.fn.name in FLIP) {
    const [a, b] = e.args as [Expr, Expr];
    let target: Expr, op: string, lit: Value;
    if (b instanceof Literal) [target, op, lit] = [a, e.fn.name, b.value];
    else if (a instanceof Literal) [target, op, lit] = [b, FLIP[e.fn.name]!, a.value];
    else return false;
    const col = columnOf(target, columns);
    return col !== null && skipCompare(col, op, lit, file);
  }
  if (isCall(e, "in")) {
    const [target, ...list] = e.args;
    const col = columnOf(target!, columns);
    return col !== null && list.every((x) => x instanceof Literal && skipCompare(col, "=", x.value, file));
  }
  if (isCall(e, "is_null") || isCall(e, "is_not_null")) {
    const col = columnOf(e.args[0]!, columns);
    const stats = col && file.stats.get(col.name);
    if (!stats) return false;
    return e.fn.name === "is_null" ? stats.nullCount === 0 : stats.nullCount === file.rowCount;
  }
  if (e instanceof ColumnRef && e.type === "bool") {
    const stats = file.stats.get(columns.get(e.id) ?? "");
    return stats !== undefined && stats.max !== true;
  }
  return false;
}

function skipCompare(col: ScanColumn, op: string, lit: Value, file: DataFile): boolean {
  const stats = file.stats.get(col.name);
  if (!stats) return false;
  if (lit === null) return true; // a comparison with null is never true
  if (stats.min === null) return true; // only nulls
  const lo = col.convert(stats.min)!, hi = col.convert(stats.max)!;
  switch (op) {
    case "=":
      if (compareValues(lit, lo) < 0 || compareValues(lit, hi) > 0) return true;
      return col.isString && stats.bloom !== null && !stats.bloom.mightContain(lit);
    case "<>": return compareValues(lo, hi) === 0 && compareValues(lo, lit) === 0;
    case "<": return compareValues(lo, lit) >= 0;
    case "<=": return compareValues(lo, lit) > 0;
    case ">": return compareValues(hi, lit) <= 0;
    case ">=": return compareValues(hi, lit) < 0;
  }
  return false;
}

export function scanColumns(scan: Scan): Map<number, string> {
  return new Map(scan.schema.map((f, i) => [f.id, scan.info.columns[i]!]));
}

export class FilePruning implements Rule {
  readonly name = "file_pruning";
  readonly doc = "Skips data files whose column statistics (min/max, null counts, bloom filters) show they hold no row the filter keeps.";

  apply(plan: PlanNode) {
    return rewritePlan(plan, (node) => {
      if (!(node instanceof Filter) || !(node.child instanceof Scan)) return null;
      const scan = node.child;
      const columns = scanColumns(scan);
      const parts = conjuncts(node.condition);
      const keep = scan.info.files.filter((f) => !parts.some((c) => canSkip(c, columns, f)));
      if (keep.length === scan.info.files.length) return null;
      return new Filter(scan.derive({ files: keep }), node.condition);
    });
  }
}
