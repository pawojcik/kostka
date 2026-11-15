// A worker's memory: what it holds, and the decision to spill. Pieces of output
// buffers waiting for their readers, hash tables, groups and sort buffers are held
// in memory. When something doesn't fit, pieces are spilled to the local disk,
// oldest first; a piece that still doesn't fit goes straight to disk; any other
// state that doesn't fit fails the query with OutOfMemoryError. The disk has a capacity
// too: a piece that doesn't fit there fails the query with OutOfDiskError.
//
// Writing to disk takes time: the pool tells whoever is running (the unit's
// context) how many bytes it spilled, and the infrastructure charges the time.

import { OutOfDiskError, OutOfMemoryError, internal } from "../../core/errors.ts";
import type { HoldKind } from "../../physical/context.ts";
import type { LocalPiece } from "./buffers.ts";

export function mb(bytes: number): string {
  return bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(0)} KB` : `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

interface Holding {
  key: string;
  kind: HoldKind;
  label: string;
  bytes: number;
  onDisk: boolean;
  piece: LocalPiece | null;
  hint?: string;
}

/** The unit running when memory is reserved: it pays for spilling. */
export interface SpillSink {
  /** `bytes` were written to the local disk to make room (or a piece went there). */
  spillWrite(bytes: number): void;
  /** A piece was moved to disk. */
  spilled(piece: LocalPiece): void;
}

export class MemoryPool {
  readonly worker: number;
  readonly capacity: number;
  readonly diskCapacity: number;
  memory = 0;
  peakMemory = 0;
  disk = 0;
  peakDisk = 0;
  spills = 0;
  bytesSpilled = 0;
  private readonly held = new Map<string, Holding>();

  constructor(worker: number, capacity: number, diskCapacity = Infinity) {
    this.worker = worker;
    this.capacity = capacity;
    this.diskCapacity = diskCapacity;
  }

  has(key: string): boolean {
    return this.held.has(key);
  }

  /** Reserves memory; spills pieces (oldest first) to make room. A piece that still
   * doesn't fit goes straight to disk; anything else is an OutOfMemoryError. */
  hold(run: SpillSink, key: string, kind: HoldKind, label: string, bytes: number, piece: LocalPiece | null = null, hint?: string): void {
    if (this.held.has(key)) throw internal(`W${this.worker} already holds ${key}`);
    const h: Holding = { key, kind, label, bytes, onDisk: false, piece, hint };
    if (this.memory + bytes > this.capacity) {
      const spilled = this.makeRoom(run, bytes);
      if (this.memory + bytes > this.capacity) {
        if (!piece) {
          throw new OutOfMemoryError(`W${this.worker} ran out of memory: ${label} needs ${mb(bytes)}, but only ${mb(Math.max(0, this.capacity - this.memory))} of its ${mb(this.capacity)} are free after spilling ${mb(spilled)} to disk. Give the workers more memory${hint ?? ", or use more shuffle partitions so that each holds less."}`);
        }
        this.held.set(key, h);
        this.spill(run, h, false);
        return;
      }
    }
    this.held.set(key, h);
    this.memory += bytes;
    this.peakMemory = Math.max(this.peakMemory, this.memory);
  }

  grow(run: SpillSink, key: string, bytes: number): void {
    const h = this.held.get(key);
    if (!h) throw internal(`W${this.worker} does not hold ${key}`);
    if (bytes <= h.bytes) return;
    this.held.delete(key);
    this.memory -= h.bytes;
    this.hold(run, key, h.kind, h.label, bytes, null, h.hint);
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
      if (h.piece && !h.onDisk) {
        this.spill(run, h, true);
        spilled += h.bytes;
      }
    }
    return spilled;
  }

  private spill(run: SpillSink, h: Holding, counted: boolean): void {
    if (this.disk + h.bytes > this.diskCapacity) {
      throw new OutOfDiskError(`W${this.worker} ran out of disk: spilling ${h.label} needs ${mb(h.bytes)}, but only ${mb(Math.max(0, this.diskCapacity - this.disk))} of its ${mb(this.diskCapacity)} disk are free. Give the workers more memory or disk, or more workers.`);
    }
    if (counted) this.memory -= h.bytes;
    h.onDisk = true;
    h.piece!.spilled = true;
    this.disk += h.bytes;
    this.peakDisk = Math.max(this.peakDisk, this.disk);
    this.bytesSpilled += h.bytes;
    this.spills++;
    run.spillWrite(h.bytes);
    run.spilled(h.piece!);
  }

  state() {
    const items = [...this.held.values()].map((h) => ({
      kind: h.kind, label: h.label, bytes: h.bytes, on_disk: h.onDisk, pipeline: Number(/^P(\d+)/.exec(h.label)?.[1] ?? -1),
    }));
    const group = (xs: typeof items) => {
      const out = new Map<string, { kind: string; label: string; bytes: number; count: number }>();
      for (const x of xs) {
        const g = out.get(x.label) ?? { kind: x.kind, label: x.label, bytes: 0, count: 0 };
        g.bytes += x.bytes;
        g.count++;
        out.set(x.label, g);
      }
      return [...out.values()];
    };
    // Neighbors of one label and kind (pieces of one buffer, say) as one strip item.
    const strip = (xs: typeof items) => {
      const out: { kind: string; label: string; bytes: number; count: number; pipeline: number }[] = [];
      for (const x of xs) {
        const last = out[out.length - 1];
        if (last && last.label === x.label && last.kind === x.kind) {
          last.bytes += x.bytes;
          last.count++;
        } else if (out.length < 200) out.push({ kind: x.kind, label: x.label, bytes: x.bytes, count: 1, pipeline: x.pipeline });
      }
      return out;
    };
    const mem = items.filter((x) => !x.on_disk), disk = items.filter((x) => x.on_disk);
    return {
      memory: { capacity: this.capacity, used: this.memory, peak: this.peakMemory, held: group(mem), items: strip(mem) },
      disk: { capacity: this.diskCapacity, used: this.disk, peak: this.peakDisk, held: group(disk), items: strip(disk) },
      spills: this.spills,
    };
  }
}
