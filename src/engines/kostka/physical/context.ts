// What a worker gives the operators of a split while it runs: the ExecContext.
// Operators never see workers, clocks, other splits or the coordinator; they report
// the work they do (CPU, IO, memory, pieces they write, keys they collect, result
// rows) and the infrastructure places it in time.
//
// Operator instances live for one split. The only thing kept across splits is the
// worker's cache (hash tables built from a build side).

import type { Value, Row } from "../../../core/types.ts";
import type { OutputBuffer, PieceRef, Split } from "./buffer.ts";
import type { FilterKeys } from "./operators/runtime-filter.ts";

export { CATEGORIES, type Category } from "../../../core/time.ts";

/** An operator of one split: what work is charged to. */
export interface Charged {
  /** The operator's id in the plan ("P2.1"). */
  readonly id: string;
  readonly metrics: import("./metrics.ts").Metrics;
}

export interface ExecContext {
  /** The worker running this split (for labels and locality). */
  readonly worker: number;
  /** The split running. */
  readonly split: Split;
  readonly pipeline: number;
  /** The most rows a batch read from a buffer may hold. */
  readonly batchRows: number;

  /** Modelled CPU time, in simulated ns (ignored when CPU time is measured). */
  cpu(ns: number, op: Charged): void;
  /** CPU time measured outside the unit, in ns: a data file decoded while it loaded
   * (decoding is async, so it cannot run inside the unit). Ignored with the model. */
  cpuTaken(ns: number, op: Charged): void;
  /** Reading `bytes` of a data file from shared table storage. */
  readStorage(bytes: number, op: Charged): void;
  /** The pieces of the outputs a buffer split reads, as the coordinator sent them. */
  pieces(): readonly PieceRef[];
  /** The rows of a piece, from the worker holding it: disk time if it was spilled,
   * network time if it is another worker. Throws PieceLostError if that worker is
   * gone. */
  fetch(piece: PieceRef, op: Charged): readonly Row[];
  /** Keeps `rows` as a piece of `partition` of the pipeline's output buffer on this
   * worker, until the buffer is released. Pieces can spill to disk. */
  store(buffer: OutputBuffer, partition: number, rows: Row[], op: Charged): PieceRef;
  /** Rows of the result: they go to the coordinator with the split's SplitDone. */
  send(rows: readonly Row[], op: Charged): void;

  /** A buffer the split reads whole (a build side): the pieces of a partition, as
   * the coordinator sent them with the split. */
  input(buffer: number, partition: number): readonly PieceRef[];
  /** A runtime filter's keys, if the coordinator sent them for this pipeline. */
  filterKeys(filter: number): ReadonlySet<Value> | null;
  /** Reports the keys this split collected for a runtime filter (at its end). */
  collectedKeys(filter: number, keys: Iterable<Value>): void;
  /** A value the worker keeps across splits until `buffer` is released: built by
   * `build` the first time (which holds its memory under `holdKey`), reused after. */
  cache<T>(buffer: number, key: string, build: (holdKey: string) => T): { value: T; hit: boolean };

  /** Reserves memory for state that cannot spill (hash tables, groups, buffers).
   * Throws OutOfMemoryError if it doesn't fit after spilling pieces. `key` must be
   * unique on the worker; `release` frees it. A label starts with "P<pipeline>". */
  hold(key: string, kind: HoldKind, label: string, bytes: number, op: Charged, hint?: string): void;
  /** Grows a held reservation to `bytes` (never shrinks). */
  grow(key: string, bytes: number, op: Charged): void;
  release(key: string): void;

  /** A line for the trace about what this unit did ("read f.parquet: 8,192 rows"). */
  note(op: Charged, text: string): void;
  /** A hash table was built: it goes into the split's report. */
  tableBuilt(op: Charged, info: { partition: number | null; rows: number; keys: number; bytes: number; again: boolean }, text: string): void;
}

export type HoldKind = "hash" | "groups" | "sort" | "buffer" | "output";
export type { FilterKeys };
