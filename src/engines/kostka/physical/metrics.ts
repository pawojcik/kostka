// Counters each operator keeps per split: rows and batches in and out, bytes, and
// simulated time by category. (Row sizes are shared by every engine: core/rows.ts.)

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
