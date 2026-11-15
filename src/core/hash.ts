// Stable hashes of values: the same in every session and in Node and the browser,
// because they are stored (bloom filters, distinct-count sketches) and decide which
// partition a row goes to. Equal values hash equally; since a value's type is fixed
// by its column, 1 (int) and 1.0 (float) are the same number and hash the same.

import type { Value } from "./types.ts";

const f64 = new Float64Array(1);
const u32 = new Uint32Array(f64.buffer);

function mix32(h: number): number {
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  return (h ^ (h >>> 16)) >>> 0;
}

function step(h: number, k: number): number {
  k = Math.imul(k, 0xcc9e2d51);
  k = (k << 15) | (k >>> 17);
  h ^= Math.imul(k, 0x1b873593);
  h = (h << 13) | (h >>> 19);
  return (Math.imul(h, 5) + 0xe6546b64) | 0;
}

/** A 32-bit hash of a non-null value (MurmurHash3's structure) with a seed. */
export function hash32(v: Value, seed = 0): number {
  let h = seed | 0;
  if (typeof v === "string") {
    let i = 0;
    for (; i + 1 < v.length; i += 2) h = step(h, v.charCodeAt(i) | (v.charCodeAt(i + 1) << 16));
    if (i < v.length) h = step(h, v.charCodeAt(i));
    return mix32(h ^ v.length ^ 0x5bd1e995);
  }
  if (typeof v === "number") {
    f64[0] = v === 0 ? 0 : v; // -0 hashes as 0
    return mix32(step(step(h, u32[0]!), u32[1]!) ^ 8);
  }
  return mix32(step(h, v ? 0x7a3f : 0x91c5) ^ 1); // booleans (nulls are never hashed)
}

/** Two independent 32-bit hashes: [hi, lo] of a 64-bit one. */
export function hash64(v: Value): [number, number] {
  return [hash32(v, 0x9747b28c), hash32(v, 0x2c1b3c6d)];
}

/** The partition of a key in [0, n). A null key goes to partition 0. */
export function partitionOf(values: readonly Value[], n: number): number {
  if (n === 1) return 0;
  let h = 0x811c9dc5;
  for (const v of values) {
    if (v === null) return 0;
    h = Math.imul(h ^ hash32(v, 0x3c6ef372), 0x01000193);
  }
  return (mix32(h) % n + n) % n;
}
