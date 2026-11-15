// A small seeded random number generator (sfc32), so generated data is the same on
// every run and in every browser.

import { hash32 } from "../core/hash.ts";

export class Random {
  private a: number;
  private b: number;
  private c: number;
  private d: number;

  constructor(seed: string | number = 0) {
    const s = String(seed);
    this.a = hash32(s, 1);
    this.b = hash32(s, 2);
    this.c = hash32(s, 3);
    this.d = 1;
    for (let i = 0; i < 12; i++) this.next32();
  }

  next32(): number {
    const t = (((this.a + this.b) | 0) + this.d) | 0;
    this.d = (this.d + 1) | 0;
    this.a = this.b ^ (this.b >>> 9);
    this.b = (this.c + (this.c << 3)) | 0;
    this.c = (this.c << 21) | (this.c >>> 11);
    this.c = (this.c + t) | 0;
    return t >>> 0;
  }

  /** [0, 1) with 53 random bits. */
  random(): number {
    return ((this.next32() >>> 5) * 67108864 + (this.next32() >>> 6)) / 9007199254740992;
  }

  /** An int in [lo, hi], both included. */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.random() * (hi - lo + 1));
  }

  uniform(lo: number, hi: number): number {
    return lo + this.random() * (hi - lo);
  }

  choice<T>(xs: readonly T[]): T {
    return xs[Math.floor(this.random() * xs.length)]!;
  }

  shuffle<T>(xs: T[]): T[] {
    for (let i = xs.length - 1; i > 0; i--) {
      const j = Math.floor(this.random() * (i + 1));
      [xs[i], xs[j]] = [xs[j]!, xs[i]!];
    }
    return xs;
  }

  /** `k` distinct ints from [lo, hi]. */
  sample(lo: number, hi: number, k: number): number[] {
    const seen = new Set<number>();
    while (seen.size < k) seen.add(this.int(lo, hi));
    return [...seen];
  }

  hex(digits: number): string {
    let s = "";
    while (s.length < digits) s += this.next32().toString(16).padStart(8, "0");
    return s.slice(0, digits);
  }
}

/** Rounds to `d` decimals (half away from zero). */
export function round(x: number, d = 2): number {
  const f = 10 ** d;
  return Math.sign(x) * Math.round(Math.abs(x) * f + 1e-9) / f;
}
