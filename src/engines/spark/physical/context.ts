// What an executor gives the operators of one task while it runs: the TaskContext.
// Operators never see executors, slots, clocks or the driver. They report the work
// they do (CPU, bytes read, memory they need) and ask for what they read (a shuffle
// partition, a broadcast), and the executor and the infrastructure place it in time.
//
// A task is self-contained: operators keep state only while it runs. What outlives
// it is the shuffle file it wrote (on its node's disk) and the broadcast values its
// executor fetched (shared by every task on that executor).

import type { Row } from "../../../core/types.ts";

/** An operator of the plan, as a task charges work to it. */
export interface Charged {
  /** The operator's id in the Spark plan. */
  readonly id: number;
}

/** A sorted run a Sort spilled to disk: only the task that spilled it reads it. */
export interface SpilledRun {
  readonly id: number;
  readonly rows: number;
  readonly bytes: number;
}

export interface TaskContext {
  /** The partition this task computes. */
  readonly partition: number;
  /** The most rows a batch holds. */
  readonly batchRows: number;

  /** Modelled CPU time, in simulated ns (ignored when CPU time is measured). */
  cpu(ns: number, op: Charged): void;
  /** CPU time measured outside the task (a data file decoded while it loaded). */
  cpuTaken(ns: number, op: Charged): void;
  /** Reading `bytes` of a data file from shared table storage. */
  readStorage(bytes: number, op: Charged): void;
  /** A data file the executor loaded for this task before it ran (FileScan). */
  loaded(op: Charged): { readonly rows: Row[]; readonly decodeNs: number } | null;

  /** The rows of reduce partition `partition` of shuffle `shuffle`, from every map
   * task's shuffle file. Throws FetchFailedError if one is gone or missing. */
  shuffleRead(shuffle: number, partition: number, op: Charged): readonly Row[];
  /** Writes this task's shuffle file: its rows, grouped by reduce partition. */
  shuffleWrite(shuffle: number, parts: readonly (readonly Row[])[], op: Charged): void;
  /** A broadcast's value on this executor: fetched from the driver and built once by
   * `build` (which says how many bytes it holds), then shared by every task. */
  broadcast<T>(broadcast: number, op: Charged, build: (rows: readonly Row[]) => { value: T; bytes: number }): T;
  /** Rows of the result (or of a broadcast job), sent to the driver with TaskFinished. */
  send(rows: readonly Row[], op: Charged): void;

  /** Grows the memory `op` holds in this task to `bytes`. False if that would pass the
   * task's share (memoryMB / cores): the operator spills, or fails. */
  reserve(op: Charged, label: string, bytes: number): boolean;
  /** Frees what `op` holds. */
  free(op: Charged): void;
  /** The memory a task may hold, in bytes. */
  readonly memoryShare: number;
  /** Writes a sorted run to the node's local disk, to read it back later. */
  spill(rows: readonly Row[], op: Charged): SpilledRun;
  /** Reads a spilled run back. */
  unspill(run: SpilledRun, op: Charged): readonly Row[];

  /** Rows an operator gave out, for the plan's metrics. */
  output(op: Charged, rows: number): void;
  /** A line about what the task did ("read f.parquet: 8,192 rows"). */
  note(op: Charged, text: string): void;
}
