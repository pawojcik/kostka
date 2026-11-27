// The CPU cost model: simulated nanoseconds per unit of work, used when a cluster's
// `cpu` setting is "model" (by default CPU time is measured; see engines/kostka/unit-context.ts).
// A model makes every run of a query take the same simulated time.
//
// The numbers are rough per-row costs of a columnar engine on one core, in the same
// proportions as measured in kostka's first (Python) implementation: a hash insert costs about 1.2
// probes, an output row about 0.3.
//
// It lives in core/ because it is a model, not infrastructure: plain numbers, with no
// clock, IO or state, so an engine's operators (its domain) can say what their work
// costs without depending on the simulated cluster.

export const COST = {
  /** Decoding one value of a data file. */
  scanValue: 2,
  /** Reading one row from a piece of an output buffer. */
  exchangeRow: 4,
  /** Evaluating one expression node for one row. */
  exprNode: 3,
  /** Per row, any streaming operator. */
  row: 2,
  hashInsert: 30,
  hashProbe: 25,
  joinOutput: 8,
  /** Per row per aggregate, folding into a group (plus a hash lookup per row). */
  aggUpdate: 6,
  /** Per row compared, times log2(rows), for sorting. */
  sortCompare: 6,
  /** Partitioning one row for a shuffle. */
  partition: 10,
} as const;

/** Nodes in an expression tree: the cost of evaluating it once. */
export function exprCost(e: { children(): readonly unknown[] }): number {
  let n = 1;
  for (const c of e.children()) n += exprCost(c as typeof e);
  return n * COST.exprNode;
}
