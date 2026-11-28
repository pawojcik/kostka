// Sizes of values and rows, in bytes: what memory holds and what IO moves. Every
// engine prices rows with these, so that their memory and IO are comparable. Sizes
// are estimates (a row is sampled), not what JavaScript actually allocates. Like the
// cost model (cost.ts), they are plain functions of values, so engines' operators and
// the cluster's memory and blocks both use them.

import type { DataType, Row, Value } from "./types.ts";

/** Bytes per row (a row index / null bitmap share) and per hash entry or group. */
export const ROW_OVERHEAD = 8;
export const ENTRY_OVERHEAD = 16;

export function valueBytes(v: Value): number {
  if (v === null || typeof v === "boolean") return 1;
  if (typeof v === "number") return 8;
  return 4 + v.length;
}

/** Estimated size of rows, from a sample of up to 16 of them. */
export function rowsBytes(rows: readonly Row[]): number {
  const n = rows.length;
  if (!n) return 0;
  const stride = Math.max(1, Math.floor(n / 16));
  let total = 0, k = 0;
  for (let i = 0; i < n && k < 16; i += stride, k++) {
    for (const v of rows[i]!) total += valueBytes(v);
  }
  return Math.round(n * (total / k + ROW_OVERHEAD));
}

export function perRowBytes(rows: readonly Row[]): number {
  return rows.length ? rowsBytes(rows) / rows.length : 0;
}

/** The characters a string value is assumed to have when only its type is known. */
export const ASSUMED_STRING_CHARS = 12;

/** Estimated bytes of a value of `type` when only the type is known: 8 for numbers,
 * dates and timestamps, 1 for a bool, 4 plus ASSUMED_STRING_CHARS for a string. */
export function typeBytes(type: DataType): number {
  if (type === "bool") return 1;
  if (type === "string") return 4 + ASSUMED_STRING_CHARS;
  return 8;
}

/** Estimated bytes of one row with these columns, before any row exists (a planner
 * sizing an estimate): the row overhead plus each column's typeBytes. */
export function fieldsBytes(fields: readonly { readonly type: DataType }[]): number {
  return fields.reduce((s, f) => s + typeBytes(f.type), ROW_OVERHEAD);
}

/** Bytes as KB or MB, for messages ("512 KB", "1.5 MB"). */
export function mb(bytes: number): string {
  return bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(0)} KB` : `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}
