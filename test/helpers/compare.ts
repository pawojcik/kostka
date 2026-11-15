// Comparing result rows: floats with a tolerance; unsorted results as multisets.
import assert from "node:assert/strict";
import type { Row, Value } from "../../src/core/types.ts";

export function same(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isInteger(a) && Number.isInteger(b)) return a === b;
    return Math.abs(a - b) <= Math.max(1e-6, 1e-9 * Math.max(Math.abs(a), Math.abs(b)));
  }
  return a === b;
}

export function assertRows(got: readonly Row[], want: readonly (readonly unknown[])[], message = ""): void {
  assert.equal(got.length, want.length, `${message}: ${got.length} rows, want ${want.length}`);
  got.forEach((g, i) => {
    const w = want[i]!;
    assert.ok(g.length === w.length && g.every((v, j) => same(v, w[j])), `${message}: row ${i}: got ${JSON.stringify(g)}, want ${JSON.stringify(w)}`);
  });
}

const key = (r: readonly unknown[]) => JSON.stringify(r.map((v) => (typeof v === "number" && !Number.isInteger(v) ? Number(v.toPrecision(9)) : v)));

/** Rows in any order. */
export function assertSameRows(got: readonly Row[], want: readonly (readonly Value[])[], message = ""): void {
  const sort = (rows: readonly (readonly unknown[])[]) => [...rows].sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
  assertRows(sort(got) as Row[], sort(want), message);
}
