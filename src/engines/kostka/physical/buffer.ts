// Output buffers carry rows from one pipeline to the pipelines after it, and splits
// are the units of work a pipeline's input is cut into.
//
// An output buffer belongs to the pipeline whose sink writes it, but its contents are
// partial and spread out: each worker holds what the splits it ran wrote, as
// *pieces* (rows of one partition, written at once), in memory or on its disk, until
// the buffer is released. Everyone else knows a piece only by its PieceRef: where it
// is and how big, never its rows.
//
// What one split wrote to one partition is an *output*, named (split, partition):
// the coordinator's unit of lineage. A buffer split names the outputs it reads, not
// their pieces, so when an output is made again after a failure (new pieces, on
// another worker) the splits reading it stay the same.
//
// A buffer has a mode:
//   shuffle    rows go to one of N partitions by a hash of their key
//   broadcast  one partition, read whole by every split of the reader
//   gather     one partition, read as one split
// Buffers whose partition p must meet in one place (a shuffle join's build and probe
// sides) share a *partitioning*: the coordinator gives each of its partitions one
// owner worker.

import type { Field } from "../../../core/expr.ts";
import type { DataFile } from "../../../catalog/meta.ts";

export type BufferMode = "shuffle" | "broadcast" | "gather";

/** A pipeline's output buffer, as the plan describes it. Holds no rows. */
export class OutputBuffer {
  readonly index: number;
  readonly mode: BufferMode;
  readonly n: number;
  readonly columns: readonly Field[];
  readonly label: string;
  /** The buffers with the same partitioning have the same partition owners. */
  partitioning: number;
  producer = -1;
  readonly consumers: number[] = [];
  /** Its readers need only this many rows (a Limit's gather): once it holds them,
   * the producer's other splits are skipped. */
  enough: number | null = null;

  constructor(index: number, mode: BufferMode, n: number, columns: readonly Field[], label: string) {
    this.index = index;
    this.mode = mode;
    this.n = n;
    this.columns = columns;
    this.label = label;
    this.partitioning = index;
  }

  get name(): string {
    return `B${this.index}`;
  }

  describe(): string {
    return this.mode === "shuffle" ? `shuffle into ${this.n} partition${this.n === 1 ? "" : "s"}` : this.mode;
  }
}

/** Where a piece is: what the coordinator and other workers know of it. */
export interface PieceRef {
  /** Unique on its worker. */
  readonly id: number;
  readonly buffer: number;
  readonly partition: number;
  readonly worker: number;
  /** The split that wrote it. */
  readonly split: number;
  readonly rows: number;
  readonly bytes: number;
}

/** What split `split` wrote to `partition` of a buffer. */
export interface OutputRef {
  readonly split: number;
  readonly partition: number;
}

export const outputKey = (o: OutputRef) => `${o.split}:${o.partition}`;

export type Split = FileSplit | BufferSplit;

interface SplitBase {
  readonly id: number;
  readonly pipeline: number;
  readonly rows: number;
  readonly bytes: number;
  readonly label: string;
}

export interface FileSplit extends SplitBase {
  readonly kind: "file";
  readonly file: DataFile;
  readonly partition: null;
}

export interface BufferSplit extends SplitBase {
  readonly kind: "buffer";
  readonly buffer: number;
  readonly partition: number;
  /** The outputs it reads, all of this partition. */
  readonly outputs: readonly OutputRef[];
  /** Part `part` of `parts` when a partition was cut into several splits. */
  readonly part: number;
  readonly parts: number;
}
