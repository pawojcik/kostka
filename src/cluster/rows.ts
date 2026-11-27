// Sizes of values and rows, in bytes: what memory holds and what IO moves. Every
// engine prices rows with these, so that their memory and IO are comparable. Sizes
// are estimates (a row is sampled), not what JavaScript actually allocates.

import type { Row, Value } from "../core/types.ts";

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
