// Runtime filters. The planner wires them: an inner join's build pipeline collects
// the values of one join key, and the probe pipeline's scan reads that key's column.
// When the build splits finish they report the keys they saw; the coordinator merges
// them, skips the probe scan's files whose min/max cannot hold a key, and sends the
// keys to the probe splits, which (when another join runs first in the probe
// pipeline) drop rows whose key is not among them, right after the scan.
//
// The spec holds no keys: they travel in messages as FilterKeys.

import { compareValues, type Value } from "../../core/types.ts";
import type { ColumnStats } from "../../catalog/meta.ts";

export class RuntimeFilter {
  readonly id: number;
  /** Index of the key in the join's key tuple. */
  readonly position: number;
  readonly label: string;
  /** The pipeline whose sink collects the keys, and the scan pipeline using them. */
  readonly producer: number;
  consumer = -1;
  /** The scanned column it tests. */
  readonly column: string;

  constructor(id: number, position: number, label: string, producer: number, column: string) {
    this.id = id;
    this.position = position;
    this.label = label;
    this.producer = producer;
    this.column = column;
  }
}

/** A filter's keys: collected by one split, or merged over all of them. */
export interface FilterKeys {
  readonly filter: number;
  readonly keys: readonly Value[];
  readonly lo: Value;
  readonly hi: Value;
}

/** The keys a split saw, with their range. */
export function summarize(filter: number, keys: Iterable<Value>): FilterKeys {
  const all = [...keys];
  let lo: Value = null, hi: Value = null;
  for (const k of all) {
    if (k === null || typeof k === "boolean") continue;
    if (lo === null || compareValues(k, lo) < 0) lo = k;
    if (hi === null || compareValues(k, hi) > 0) hi = k;
  }
  return { filter, keys: all, lo, hi };
}

export function mergeKeys(filter: number, parts: readonly FilterKeys[]): FilterKeys {
  const set = new Set<Value>();
  for (const p of parts) for (const k of p.keys) set.add(k);
  return summarize(filter, set);
}

/** True if a data file with these stats cannot hold one of the keys. */
export function misses(f: FilterKeys, stats: ColumnStats | undefined): boolean {
  if (!f.keys.length) return true;
  if (!stats) return false;
  if (stats.min === null) return true; // only nulls, which never match
  if (f.lo === null || typeof stats.min !== typeof f.lo) return false;
  return compareValues(stats.max, f.lo) < 0 || compareValues(stats.min, f.hi) > 0;
}
