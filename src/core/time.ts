// Where simulated time goes: the vocabulary the cluster (which keeps the time) and
// every engine (which reports its work) share. A node's idle time is the rest of the
// query's time.

export const CATEGORIES = ["cpu", "storage", "network", "spill_write", "spill_read"] as const;
export type Category = (typeof CATEGORIES)[number];

/** A record with 0 for every category. */
export function zeroTime(): Record<Category, number> {
  return { cpu: 0, storage: 0, network: 0, spill_write: 0, spill_read: 0 };
}
