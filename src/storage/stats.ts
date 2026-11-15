// Per-column statistics of a data file, computed when it is written: min, max, null
// count, exact distinct count, a HyperLogLog sketch (a float column only when the file
// has at most FLOAT_SKETCH_LIMIT distinct values: hashing is the costly part, and a
// column with many values per file is close enough to unique), and for string columns
// a bloom filter.

import { compareValues, type ColumnType, type Value } from "../core/types.ts";
import type { ColumnStats } from "../catalog/meta.ts";
import { BloomFilter, HyperLogLog } from "./sketch.ts";

export const FLOAT_SKETCH_LIMIT = 1024;

export function columnStats(type: ColumnType, values: readonly Value[]): ColumnStats {
  let min: Value = null, max: Value = null, nulls = 0;
  const distinct = new Set<Value>();
  for (const v of values) {
    if (v === null) {
      nulls++;
      continue;
    }
    distinct.add(v);
    if (min === null || compareValues(v, min) < 0) min = v;
    if (max === null || compareValues(v, max) > 0) max = v;
  }
  if (type === "bool") return { min, max, nullCount: nulls, distinct: distinct.size, bloom: null, sketch: null };
  return {
    min, max, nullCount: nulls, distinct: distinct.size,
    bloom: type === "string" ? BloomFilter.of(distinct, distinct.size) : null,
    sketch: type !== "float" || distinct.size <= FLOAT_SKETCH_LIMIT ? HyperLogLog.of(distinct) : null,
  };
}
