// A node's memory and disk: what it holds, and the decision to spill. Infrastructure,
// shared by every engine. An engine holds two kinds of things:
//
// - blocks (blocks.ts): rows waiting for their readers, which can spill;
// - state that cannot spill: hash tables, groups, sort buffers.
//
// When something doesn't fit, blocks are spilled to the local disk, oldest first; a
// block that still doesn't fit goes straight to disk; any other state that doesn't
// fit fails the unit with OutOfMemoryError. The disk has a capacity too: a block that
// doesn't fit there fails the unit with OutOfDiskError. Each error ends with the
// engine's advice on what to change.
//
// Writing to disk takes time: the pool tells whoever is running (the unit's
// context) how many bytes it spilled, and the unit charges the time.

import { OutOfDiskError, OutOfMemoryError, internal } from "../core/errors.ts";
import type { Block } from "./blocks.ts";

export function mb(bytes: number): string {
  return bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(0)} KB` : `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

interface Holding {
  key: string;
  /** What kind of state it is, in the engine's words ("hash", "output"). */
  kind: string;
  label: string;
  bytes: number;
  onDisk: boolean;
  block: Block | null;
  /** What to do if it doesn't fit (the end of the error's message). */
  advice: string;
}

/** The unit running when memory is reserved: it pays for spilling. (kostka's worker
 * has an interface of the same name and shape, worker/worker.ts: the domain's own
 * port, which this one satisfies.) */
export interface SpillSink {
  /** `bytes` were written to the local disk to make room (or a block went there). */
  spillWrite(bytes: number): void;
}

/** What a pool holds, for the workbench. */
export interface MemoryState {
  readonly memory: { readonly capacity: number; readonly used: number; readonly peak: number; readonly held: readonly HeldGroup[]; readonly items: readonly HeldGroup[] };
  readonly disk: { readonly capacity: number; readonly used: number; readonly peak: number; readonly held: readonly HeldGroup[]; readonly items: readonly HeldGroup[] };
  readonly spills: number;
}

/** Things held under one label (and kind): their bytes and how many. */
export interface HeldGroup {
  readonly kind: string;
  readonly label: string;
  readonly bytes: number;
  readonly count: number;
}

export class MemoryPool {
  /** Who holds it, for errors ("W0"). */
  readonly owner: string;
  readonly capacity: number;
  readonly diskCapacity: number;
  /** What to do when the disk is full (the end of OutOfDiskError's message). */
  private readonly diskAdvice: string;
  memory = 0;
  peakMemory = 0;
  disk = 0;
  peakDisk = 0;
  spills = 0;
  bytesSpilled = 0;
  private readonly held = new Map<string, Holding>();

  constructor(owner: string, capacity: number, diskCapacity = Infinity, diskAdvice = "Give the nodes more memory or disk.") {
    this.owner = owner;
    this.capacity = capacity;
    this.diskCapacity = diskCapacity;
    this.diskAdvice = diskAdvice;
  }

  has(key: string): boolean {
    return this.held.has(key);
  }

  /** Reserves memory for `block` (which can spill) or for state that cannot (no
   * block); spills blocks (oldest first) to make room. A block that still doesn't fit
   * goes straight to disk; anything else is an OutOfMemoryError ending in `advice`. */
  hold(run: SpillSink, key: string, kind: string, label: string, bytes: number, block: Block | null, advice: string): void {
    if (this.held.has(key)) throw internal(`${this.owner} already holds ${key}`);
    const h: Holding = { key, kind, label, bytes, onDisk: false, block, advice };
    if (this.memory + bytes > this.capacity) {
      const spilled = this.makeRoom(run, bytes);
      if (this.memory + bytes > this.capacity) {
        if (!block) {
          throw new OutOfMemoryError(`${this.owner} ran out of memory: ${label} needs ${mb(bytes)}, but only ${mb(Math.max(0, this.capacity - this.memory))} of its ${mb(this.capacity)} are free after spilling ${mb(spilled)} to disk. ${advice}`);
        }
        this.held.set(key, h);
        this.spill(run, h, block, false);
        return;
      }
    }
    this.held.set(key, h);
    this.memory += bytes;
    this.peakMemory = Math.max(this.peakMemory, this.memory);
  }

  grow(run: SpillSink, key: string, bytes: number): void {
    const h = this.held.get(key);
    if (!h) throw internal(`${this.owner} does not hold ${key}`);
    if (bytes <= h.bytes) return;
    this.held.delete(key);
    this.memory -= h.bytes;
    this.hold(run, key, h.kind, h.label, bytes, null, h.advice);
  }

  release(key: string): void {
    const h = this.held.get(key);
    if (!h) return;
    this.held.delete(key);
    if (h.onDisk) this.disk -= h.bytes;
    else this.memory -= h.bytes;
  }

  private makeRoom(run: SpillSink, bytes: number): number {
    let spilled = 0;
    for (const h of this.held.values()) {
      if (this.memory + bytes <= this.capacity) break;
      if (h.block && !h.onDisk) {
        this.spill(run, h, h.block, true);
        spilled += h.bytes;
      }
    }
    return spilled;
  }

  private spill(run: SpillSink, h: Holding, block: Block, counted: boolean): void {
    if (this.disk + h.bytes > this.diskCapacity) {
      throw new OutOfDiskError(`${this.owner} ran out of disk: spilling ${h.label} needs ${mb(h.bytes)}, but only ${mb(Math.max(0, this.diskCapacity - this.disk))} of its ${mb(this.diskCapacity)} disk are free. ${this.diskAdvice}`);
    }
    if (counted) this.memory -= h.bytes;
    h.onDisk = true;
    block.spilled = true;
    this.disk += h.bytes;
    this.peakDisk = Math.max(this.peakDisk, this.disk);
    this.bytesSpilled += h.bytes;
    this.spills++;
    run.spillWrite(h.bytes);
  }

  state(): MemoryState {
    const items = [...this.held.values()];
    const group = (xs: readonly Holding[]): HeldGroup[] => {
      const out = new Map<string, { kind: string; label: string; bytes: number; count: number }>();
      for (const x of xs) {
        const g = out.get(x.label) ?? { kind: x.kind, label: x.label, bytes: 0, count: 0 };
        g.bytes += x.bytes;
        g.count++;
        out.set(x.label, g);
      }
      return [...out.values()];
    };
    // Neighbors of one label and kind (blocks of one output, say) as one strip item.
    const strip = (xs: readonly Holding[]): HeldGroup[] => {
      const out: { kind: string; label: string; bytes: number; count: number }[] = [];
      for (const x of xs) {
        const last = out[out.length - 1];
        if (last && last.label === x.label && last.kind === x.kind) {
          last.bytes += x.bytes;
          last.count++;
        } else if (out.length < 200) out.push({ kind: x.kind, label: x.label, bytes: x.bytes, count: 1 });
      }
      return out;
    };
    const mem = items.filter((x) => !x.onDisk), disk = items.filter((x) => x.onDisk);
    return {
      memory: { capacity: this.capacity, used: this.memory, peak: this.peakMemory, held: group(mem), items: strip(mem) },
      disk: { capacity: this.diskCapacity, used: this.disk, peak: this.peakDisk, held: group(disk), items: strip(disk) },
      spills: this.spills,
    };
  }
}
