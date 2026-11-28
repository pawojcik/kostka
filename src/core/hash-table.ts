// A hash table of rows by their join key: the build side of a hash join, in every
// engine. Rows are added in batches with the evaluators of their key; a row with a
// null key value never matches, so it is only counted. Its size in bytes is an
// estimate (core/rows.ts), what memory holds for it.
//
// With no keys (a join without an equality) every row goes into one bucket.

import type { Evaluator } from "./compile.ts";
import { tupleKey, type Row, type Value } from "./types.ts";
import { ENTRY_OVERHEAD, perRowBytes } from "./rows.ts";

export class HashTable {
  readonly buckets = new Map<Value, Row[]>();
  rows = 0;
  nullKeys = 0;
  bytes = 0;
  private perRow = 0;

  add(rows: readonly Row[], keys: readonly Evaluator[]): void {
    for (const row of rows) {
      const k = keyOf(row, keys);
      if (k === undefined) {
        this.nullKeys++;
        continue;
      }
      const b = this.buckets.get(k);
      if (b) b.push(row);
      else this.buckets.set(k, [row]);
      this.rows++;
    }
    if (rows.length && !this.perRow) this.perRow = perRowBytes(rows);
    this.bytes = Math.round(this.rows * this.perRow + this.buckets.size * ENTRY_OVERHEAD);
  }
}

/** The key of a row, or undefined if any key value is null (it never matches). */
export function keyOf(row: Row, keys: readonly Evaluator[]): Value | undefined {
  if (keys.length === 0) return 0; // no equality keys: one bucket
  if (keys.length === 1) {
    const v = keys[0]!(row);
    return v === null ? undefined : v;
  }
  const vs: Value[] = [];
  for (const k of keys) {
    const v = k(row);
    if (v === null) return undefined;
    vs.push(v);
  }
  return tupleKey(vs);
}
