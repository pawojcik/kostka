// A value as it changed, step by step: what an engine's scheduler keeps of each thing
// it knows (a split's status, a task's), so the workbench can show the state as it
// was right after any step. Setting it twice at one step keeps the last value.
// Domain-neutral: plain values and step numbers, no clock.

/** A value as it changed, step by step. */
export class History<T> {
  private readonly at: number[] = [];
  private readonly values: T[] = [];
  constructor(seq: number, v: T) {
    this.set(seq, v);
  }
  set(seq: number, v: T): void {
    if (this.at.length && this.at[this.at.length - 1] === seq) this.values[this.values.length - 1] = v;
    else {
      this.at.push(seq);
      this.values.push(v);
    }
  }
  get now(): T {
    return this.values[this.values.length - 1]!;
  }
  /** The value after step `seq`, or undefined before it existed. */
  after(seq: number): T | undefined {
    let lo = 0, hi = this.at.length - 1, best = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (this.at[mid]! <= seq) {
        best = mid;
        lo = mid + 1;
      } else hi = mid - 1;
    }
    return best < 0 ? undefined : this.values[best];
  }
  get since(): number {
    return this.at[0]!;
  }
}
