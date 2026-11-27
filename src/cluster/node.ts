// A node: one simulated machine of the cluster. Infrastructure: engines place their
// own workers on nodes, and the node keeps the time.
//
// A node has `cores` cores, one IO channel (its disk and network bandwidth), its
// memory and its local block store. Each core has its own clock and runs one unit of
// work at a time. A unit is laid out on its core's timeline as segments
// [kind, start, end]:
//
// - CPU segments run on the core's clock, so units on different cores overlap.
// - Storage, network and spill segments also need the IO channel. Such a segment
//   starts at the earliest time from the core's clock on when the channel is free for
//   as long as it takes, and holds the channel until it ends. The time spent waiting
//   is a "wait" segment, so the cost of sharing bandwidth shows on the timeline.
//
// With one core in use nothing waits for the channel: a core's segments never
// overlap each other.
//
// Invariants: a core's clock never goes back; a unit's segments are in order, do not
// overlap, and end at the unit's end; the channel is never booked twice at once.

import { internal } from "../core/errors.ts";
import { CATEGORIES, zeroTime, type Category } from "../core/time.ts";
import { ioCost, type IOProfile } from "./config.ts";
import type { BlockStore } from "./blocks.ts";
import type { MemoryPool, MemoryState } from "./memory.ts";

export { CATEGORIES, type Category };
/** The categories that use the IO channel. */
export type IoCategory = Exclude<Category, "cpu">;
/** A segment's kind: a category, or waiting for the IO channel. */
export type SegmentKind = Category | "wait";
export type Segment = [kind: SegmentKind, start: number, end: number];

/** Bytes a node moved, by where they went. */
export const BYTE_COUNTERS = ["storage_read", "network_in", "network_out", "spill_written", "spill_read"] as const;
export type ByteCounter = (typeof BYTE_COUNTERS)[number];

const DEVICE: Record<IoCategory, "storage" | "network" | "disk"> = { storage: "storage", network: "network", spill_write: "disk", spill_read: "disk" };


/** Simulated ns by category, in ms. */
function msOf(time: Readonly<Record<Category, number>>): Record<Category, number> {
  const out = zeroTime();
  for (const c of CATEGORIES) out[c] = time[c] / 1e6;
  return out;
}

/** The IO channel: the times it is booked, so that segments of units on different
 * cores take turns. */
export class IoChannel {
  /** Booked intervals [start, end), sorted, not overlapping. */
  private readonly booked: [number, number][] = [];

  /** Books the channel for `ns` at the earliest time from `from` on when it is free
   * that long; returns that start. Intervals that end before `floor` are forgotten:
   * nothing can be booked before it any more. */
  book(from: number, ns: number, floor: number): number {
    while (this.booked[0] && this.booked[0][1] <= floor) this.booked.shift();
    let start = from, i = 0;
    for (const [s, e] of this.booked) {
      if (s >= start + ns) break; // the gap before this interval is long enough
      if (e > start) start = e; // it overlaps: try right after it
      i++;
    }
    this.booked.splice(i, 0, [start, start + ns]);
    // an interval that touches its neighbour becomes one with it: the list stays short
    const touch = (j: number) => {
      const a = this.booked[j], b = this.booked[j + 1];
      if (!a || !b || a[1] !== b[0]) return false;
      a[1] = b[1];
      this.booked.splice(j + 1, 1);
      return true;
    };
    touch(i);
    if (i > 0) touch(i - 1);
    return start;
  }
}

/** A node as the workbench shows it, right after a step. */
export interface NodeFrame {
  readonly node: number;
  readonly status: "up" | "gone";
  /** Each core's clock: when it is free for its next unit. */
  readonly cores: readonly { readonly clock_ms: number }[];
  /** The latest core's clock. */
  readonly clock_ms: number;
  readonly busy_ms: number;
  /** Units that finished. */
  readonly runs: number;
  readonly time_ms: Readonly<Record<Category, number>>;
  readonly bytes: Readonly<Record<ByteCounter, number>>;
  readonly memory: MemoryState["memory"];
  readonly disk: MemoryState["disk"];
  readonly spills: number;
}

export class Node {
  readonly id: number;
  readonly profile: IOProfile;
  /** Each core's clock, in simulated ns. */
  readonly clocks: number[];
  readonly channel = new IoChannel();
  readonly memory: MemoryPool;
  readonly blocks: BlockStore;
  /** False once the node is gone (stopped), with everything it held. */
  up = true;
  runs = 0;
  readonly time: Record<Category, number> = zeroTime();
  readonly bytes: Record<ByteCounter, number> = { storage_read: 0, network_in: 0, network_out: 0, spill_written: 0, spill_read: 0 };
  /** The unit running on each core, if one is being laid out. */
  private readonly running: (Unit | null)[];

  constructor(id: number, cores: number, profile: IOProfile, memory: MemoryPool, blocks: BlockStore) {
    if (!Number.isInteger(cores) || cores < 1) throw internal(`a node needs at least one core, got ${cores}`);
    this.id = id;
    this.profile = profile;
    this.clocks = Array.from({ length: cores }, () => 0);
    this.running = this.clocks.map(() => null);
    this.memory = memory;
    this.blocks = blocks;
  }

  /** When the node is free: its latest core's clock. */
  get clock(): number {
    return Math.max(...this.clocks);
  }

  get busy(): number {
    return CATEGORIES.reduce((s, c) => s + this.time[c], 0);
  }

  /** Starts laying out a unit on `core` at `start` (not before the core's clock). */
  begin(core: number, start: number): Unit {
    const clock = this.clocks[core];
    if (clock === undefined) throw internal(`node ${this.id} has no core ${core}`);
    if (this.running[core]) throw internal(`core ${core} of node ${this.id} is running a unit already`);
    if (start < clock) throw internal(`a unit cannot start on core ${core} of node ${this.id} at ${start} ns, before its clock (${clock} ns)`);
    const unit = new Unit(this, core, start);
    this.running[core] = unit;
    return unit;
  }

  /** Called by a unit when it ends. */
  ended(unit: Unit, finished: boolean): void {
    this.running[unit.core] = null;
    this.clocks[unit.core] = unit.cursor;
    if (!finished) return;
    this.runs++;
    for (const c of CATEGORIES) this.time[c] += unit.time[c];
  }

  /** The node's IO channel, from `from` for `ns`; returns when the segment starts. */
  bookChannel(from: number, ns: number): number {
    return this.channel.book(from, ns, Math.min(...this.clocks));
  }

  frame(): NodeFrame {
    const m = this.memory.state();
    return {
      node: this.id, status: this.up ? "up" : "gone", cores: this.clocks.map((c) => ({ clock_ms: c / 1e6 })), clock_ms: this.clock / 1e6,
      busy_ms: this.busy / 1e6, runs: this.runs,
      time_ms: msOf(this.time),
      bytes: { ...this.bytes, spill_written: this.memory.bytesSpilled },
      memory: m.memory, disk: m.disk, spills: m.spills,
    };
  }
}

/** Where a unit's time went. */
export interface UnitTiming {
  readonly start: number;
  readonly end: number;
  readonly time: Record<Category, number>;
  readonly segments: Segment[];
}

/** A unit of work being laid out on one core of a node: the engine reports its CPU and
 * IO as it runs, and the unit places them in time. */
export class Unit {
  readonly node: Node;
  readonly core: number;
  readonly start: number;
  /** Now, on the core's timeline. */
  cursor: number;
  readonly segments: Segment[] = [];
  readonly time: Record<Category, number> = zeroTime();
  private over = false;

  constructor(node: Node, core: number, start: number) {
    this.node = node;
    this.core = core;
    this.start = this.cursor = start;
  }

  private put(kind: SegmentKind, start: number, ns: number): void {
    const last = this.segments[this.segments.length - 1];
    if (last && last[0] === kind && last[2] === start) last[2] += ns;
    else this.segments.push([kind, start, start + ns]);
    this.cursor = start + ns;
  }

  /** `ns` of CPU time, now. */
  cpu(ns: number): number {
    if (this.over) throw internal("this unit has ended");
    if (ns <= 0) return 0;
    this.put("cpu", this.cursor, ns);
    this.time.cpu += ns;
    return ns;
  }

  /** Moves `bytes` on the device of `kind`, priced by the IO profile and counted in
   * `counter`; waits for the IO channel if another core holds it. Returns the ns the
   * IO took (not counting the wait). */
  io(kind: IoCategory, bytes: number, counter: ByteCounter): number {
    if (this.over) throw internal("this unit has ended");
    this.node.bytes[counter] += bytes;
    const ns = ioCost(this.node.profile, DEVICE[kind], bytes);
    if (ns <= 0) return 0;
    const start = this.node.bookChannel(this.cursor, ns);
    if (start > this.cursor) this.put("wait", this.cursor, start - this.cursor);
    this.put(kind, start, ns);
    this.time[kind] += ns;
    return ns;
  }

  /** Ends the unit: its core is free from the unit's end. A unit that `finished`
   * counts in the node's runs and time; one that failed only took the time. */
  end(finished: boolean): UnitTiming {
    if (this.over) throw internal("this unit has ended already");
    this.over = true;
    this.node.ended(this, finished);
    return { start: this.start, end: this.cursor, time: { ...this.time }, segments: this.segments };
  }
}
