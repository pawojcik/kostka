// Ordering rows for ORDER BY, in every engine: comparing evaluated sort keys (ASC or
// DESC, NULLS FIRST or LAST), keeping the best n rows of a stream (TopK), and picking
// the next row of a k-way merge of sorted runs (bestRun). Rows with equal keys may
// come out in any order: only the sort keys are ordered.

import type { SortKey } from "./expr.ts";
import { compareValues, type Row, type Value } from "./types.ts";

/** A row with its evaluated sort keys. */
export type Keyed = { k: Value[]; row: Row };

export type KeyCompare = (a: Value[], b: Value[]) => number;

/** A comparator over evaluated keys, honouring ASC/DESC and NULLS FIRST/LAST. */
export function keyComparator(keys: readonly SortKey[]): KeyCompare {
  return (a, b) => {
    for (let i = 0; i < keys.length; i++) {
      const x = a[i]!, y = b[i]!;
      if (x === y) continue;
      const key = keys[i]!;
      if (x === null || y === null) return (x === null) === key.nullsFirst ? -1 : 1;
      const c = compareValues(x, y);
      if (c) return key.ascending ? c : -c;
    }
    return 0;
  };
}

/** The best `n` rows offered, in a heap whose root is the worst kept. */
export class TopK {
  readonly n: number;
  private readonly cmp: KeyCompare;
  private heap: Keyed[] = []; // a max-heap by sort order: heap[0] sorts last
  seen = 0;
  replaced = 0;
  rejected = 0;

  constructor(n: number, cmp: KeyCompare) {
    this.n = n;
    this.cmp = cmp;
  }

  get size(): number {
    return this.heap.length;
  }

  offer(item: Keyed): void {
    this.seen++;
    const heap = this.heap;
    if (heap.length < this.n) {
      heap.push(item);
      this.up(heap.length - 1);
    } else if (this.n && this.cmp(item.k, heap[0]!.k) < 0) {
      heap[0] = item;
      this.down(0);
      this.replaced++;
    } else this.rejected++;
  }

  /** The rows kept, best first; the heap is empty after. */
  take(): Keyed[] {
    const best = [...this.heap].sort((a, b) => this.cmp(a.k, b.k));
    this.heap = [];
    return best;
  }

  private worse(a: Keyed, b: Keyed): boolean {
    return this.cmp(a.k, b.k) > 0;
  }

  private up(i: number): void {
    const heap = this.heap;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.worse(heap[i]!, heap[p]!)) break;
      [heap[i], heap[p]] = [heap[p]!, heap[i]!];
      i = p;
    }
  }

  private down(i: number): void {
    const heap = this.heap;
    for (;;) {
      const l = 2 * i + 1, r = l + 1;
      let m = i;
      if (l < heap.length && this.worse(heap[l]!, heap[m]!)) m = l;
      if (r < heap.length && this.worse(heap[r]!, heap[m]!)) m = r;
      if (m === i) return;
      [heap[i], heap[m]] = [heap[m]!, heap[i]!];
      i = m;
    }
  }
}

/** The run whose next row (at `heads[i]`) sorts first, or -1 when every run is used
 * up: one step of a k-way merge. The first such run wins a tie. */
export function bestRun(runs: readonly (readonly Keyed[])[], heads: readonly number[], cmp: KeyCompare): number {
  let best = -1;
  for (let i = 0; i < runs.length; i++) {
    if (heads[i]! >= runs[i]!.length) continue;
    if (best < 0 || cmp(runs[i]![heads[i]!]!.k, runs[best]![heads[best]!]!.k) < 0) best = i;
  }
  return best;
}
