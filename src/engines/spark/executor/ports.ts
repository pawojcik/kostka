// What an executor needs of the infrastructure, as interfaces the domain defines: a
// clock for each task (where its CPU and IO go in time), the node's memory and block
// store, reading a block another node holds, and fetching a broadcast from the driver.
// The simulation (sim.ts) implements them on the cluster; real IO could instead.

import type { Category } from "../../../core/time.ts";
import type { Row } from "../../../core/types.ts";

/** The unit of work running when memory is reserved: it pays for disk writes. */
export interface SpillSink {
  spillWrite(bytes: number): void;
}

/** A block of rows on a node (a shuffle file, a spilled run). */
export interface RowBlock {
  readonly id: number;
  readonly rows: readonly Row[];
  readonly bytes: number;
  /** Set by the node's memory when the block is on disk. */
  spilled: boolean;
}

/** The node's memory and disk, as an executor uses them. */
export interface NodeMemory {
  /** Reserves memory for state; throws OutOfMemoryError (ending in `advice`) if the
   * node cannot fit it. */
  hold(sink: SpillSink, key: string, kind: string, label: string, bytes: number, block: RowBlock | null, advice: string): void;
  grow(sink: SpillSink, key: string, bytes: number): void;
  /** Puts a block straight on the node's disk; throws OutOfDiskError if it is full. */
  holdOnDisk(sink: SpillSink, key: string, kind: string, label: string, block: RowBlock): void;
  release(key: string): void;
}

export interface NodeBlocks {
  put(rows: readonly Row[]): RowBlock;
  drop(id: number): void;
}

/** A block read from a node: its rows, or why there are none. */
export type BlockRead =
  | { readonly kind: "ok"; readonly rows: readonly Row[] }
  /** The node holding it is gone. */
  | { readonly kind: "gone" }
  /** The node is up but holds no such block (it was freed). */
  | { readonly kind: "missing" };

/** Where one task's time goes: its slot's core, and the node's IO channel. */
export interface TaskClock {
  /** CPU time is measured (the cost model's numbers are then ignored). */
  readonly measured: boolean;
  cpu(ns: number): void;
  /** CPU time measured outside the task (a data file decoded while it loaded). */
  cpuTaken(ns: number): void;
  readStorage(bytes: number): void;
  /** The node's local disk. */
  writeDisk(bytes: number): void;
  readDisk(bytes: number): void;
  /** Bytes to or from another node, or the driver. */
  network(bytes: number, dir: "in" | "out"): void;
  /** Ends the task, `finished` or failed: where its time went. */
  end(finished: boolean): { start: number; end: number; time: Record<Category, number>; segments: [Category | "wait", number, number][] };
}

export interface ExecutorPorts {
  /** A clock for a task in slot `slot` (on that core of the node), from `start`. */
  begin(slot: number, start: number): TaskClock;
  /** Block `block` of node `node`. */
  readBlock(node: number, block: number): BlockRead;
  /** A broadcast's rows, from the driver. */
  fetchBroadcast(broadcast: number): readonly Row[];
  readonly memory: NodeMemory;
  readonly blocks: NodeBlocks;
}
