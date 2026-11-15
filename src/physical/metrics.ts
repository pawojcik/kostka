// Counters each operator keeps per split, plus the sizes of values the workers use
// for memory and IO.

import type { Row, Value } from "../core/types.ts";
import type { Batch } from "./batch.ts";
import { CATEGORIES, type Category } from "./context.ts";

export class Metrics {
  batchesIn = 0;
  rowsIn = 0;
  batchesOut = 0;
  rowsOut = 0;
  /** Simulated ns, by category: cpu from the cost model, the rest from IO. */
  readonly time: Record<Category, number> = Object.fromEntries(CATEGORIES.map((c) => [c, 0])) as Record<Category, number>;
  bytesIn = 0;
  bytesOut = 0;
  /** Splits (and finish steps) it ran in. */
  runs = 0;

  tookIn(b: Batch): void {
    this.batchesIn++;
    this.rowsIn += b.rows.length;
  }
  gaveOut(b: Batch): void {
    this.batchesOut++;
    this.rowsOut += b.rows.length;
  }
  snapshot(): MetricsSnapshot {
    return { rowsIn: this.rowsIn, rowsOut: this.rowsOut, batchesIn: this.batchesIn, batchesOut: this.batchesOut, time: { ...this.time }, bytesIn: this.bytesIn, bytesOut: this.bytesOut };
  }
  /** What happened since `before`: one run's share. */
  since(before: MetricsSnapshot): MetricsSnapshot {
    const time = { ...this.time };
    for (const c of CATEGORIES) time[c] -= before.time[c];
    return {
      rowsIn: this.rowsIn - before.rowsIn, rowsOut: this.rowsOut - before.rowsOut,
      batchesIn: this.batchesIn - before.batchesIn, batchesOut: this.batchesOut - before.batchesOut, time, bytesIn: this.bytesIn - before.bytesIn,
      bytesOut: this.bytesOut - before.bytesOut,
    };
  }
}

export interface MetricsSnapshot {
  rowsIn: number;
  rowsOut: number;
  batchesIn: number;
  batchesOut: number;
  time: Record<Category, number>;
  bytesIn: number;
  bytesOut: number;
}

// --- sizes ----------------------------------------------------------------------

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
