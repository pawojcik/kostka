// Per-file column summaries kept in the catalog, used by the optimizer:
//
// - HyperLogLog distinct-count sketches. Each data file stores one per column (for a
//   float column only when the file has few distinct values). Sketches of several
//   files merge (register-wise max) into the sketch of their union, which exact
//   per-file counts cannot do. P = 10: 1,024 registers, about 3% error.
// - Bloom filters over string columns (~1% false positives), so `col = 'x'` can skip
//   files that certainly don't hold 'x'.
//
// Both are serialized compactly: most registers of a small file's sketch are zero.

import { hash32, hash64 } from "../core/hash.ts";
import type { Value } from "../core/types.ts";

export const HLL_P = 10;
const M = 1 << HLL_P;
const ALPHA = 0.7213 / (1 + 1.079 / M);

function clz54(hi: number, lo: number): number {
  // leading zeros of the 54 bits left after the 10 index bits: 22 of hi, then 32 of lo
  const top = hi & 0x3fffff;
  if (top) return Math.clz32(top) - 10;
  return 22 + (lo ? Math.clz32(lo) : 32);
}

export class HyperLogLog {
  readonly registers: Uint8Array;
  constructor(registers?: Uint8Array) {
    this.registers = registers ?? new Uint8Array(M);
  }

  static of(values: Iterable<Value>): HyperLogLog {
    const s = new HyperLogLog();
    for (const v of values) s.add(v);
    return s;
  }

  add(v: Value): void {
    if (v === null) return;
    const [hi, lo] = hash64(v);
    const j = hi >>> 22;
    const rank = clz54(hi, lo) + 1;
    if (rank > this.registers[j]!) this.registers[j] = rank;
  }

  merge(other: HyperLogLog): this {
    const a = this.registers, b = other.registers;
    for (let j = 0; j < M; j++) if (b[j]! > a[j]!) a[j] = b[j]!;
    return this;
  }

  static union(sketches: Iterable<HyperLogLog>): HyperLogLog {
    const out = new HyperLogLog();
    for (const s of sketches) out.merge(s);
    return out;
  }

  estimate(): number {
    let zeros = 0, sum = 0;
    for (const r of this.registers) {
      if (r === 0) zeros++;
      sum += 2 ** -r;
    }
    if (zeros === M) return 0;
    const e = (ALPHA * M * M) / sum;
    return e <= 2.5 * M ? M * Math.log(M / zeros) : e; // small range: linear counting
  }

  /** Run-length encoded: (zero run length, value) byte pairs. */
  toBytes(): Uint8Array {
    const out: number[] = [];
    let run = 0;
    for (const r of this.registers) {
      if (r === 0 && run < 255) {
        run++;
        continue;
      }
      out.push(run, r);
      run = 0;
    }
    if (run) out.push(run - 1, 0);
    return Uint8Array.from(out);
  }

  static fromBytes(bytes: Uint8Array): HyperLogLog {
    const regs = new Uint8Array(M);
    let j = 0;
    for (let i = 0; i + 1 < bytes.length; i += 2) {
      j += bytes[i]!;
      if (j < M) regs[j] = bytes[i + 1]!;
      j++;
    }
    return new HyperLogLog(regs);
  }
}

export class BloomFilter {
  readonly bits: Uint8Array;
  readonly k: number;
  constructor(bits: Uint8Array, k: number) {
    this.bits = bits;
    this.k = k;
  }

  /** Sized for `n` distinct values at about 1% false positives. */
  static of(values: Iterable<Value>, n: number): BloomFilter {
    const m = Math.max(64, Math.ceil((n * 9.6) / 8) * 8);
    const f = new BloomFilter(new Uint8Array(m / 8), 7);
    for (const v of values) if (v !== null) f.add(v);
    return f;
  }

  private *positions(v: Value): Generator<number> {
    const m = this.bits.length * 8;
    const a = hash32(v, 0x51ed270b), b = hash32(v, 0x2545f491) | 1;
    for (let i = 0; i < this.k; i++) yield ((a + Math.imul(i, b)) >>> 0) % m;
  }

  add(v: Value): void {
    for (const p of this.positions(v)) this.bits[p >>> 3]! |= 1 << (p & 7);
  }

  mightContain(v: Value): boolean {
    for (const p of this.positions(v)) if (!(this.bits[p >>> 3]! & (1 << (p & 7)))) return false;
    return true;
  }

  toBytes(): Uint8Array {
    const out = new Uint8Array(this.bits.length + 1);
    out[0] = this.k;
    out.set(this.bits, 1);
    return out;
  }

  static fromBytes(bytes: Uint8Array): BloomFilter {
    return new BloomFilter(bytes.slice(1), bytes[0]!);
  }
}
