// The messages between the driver and the executors: the only way they talk.
// Commands go from the driver to one executor; events come to the driver, from an
// executor or (ExecutorLost) from the infrastructure. They carry descriptions (where a
// shuffle file is, how many bytes it has per reduce partition, metrics), never shared
// objects; only TaskFinished carries rows: a result task's (or a broadcast job's).
//
// A real deployment would serialize them; the simulation (sim.ts) passes them as they
// are, in simulated time.

import type { Category } from "../../core/time.ts";
import type { Row } from "../../core/types.ts";

/** Where a map task's shuffle file is, and what it holds for each reduce partition:
 * Spark's MapStatus. The file is one block of rows grouped by reduce partition;
 * `offsets[p]` is where partition p starts in it (its index). */
export interface MapStatus {
  readonly node: number;
  readonly block: number;
  readonly bytes: readonly number[];
  readonly offsets: readonly number[];
}

/** A shuffle a task reads: each map task's MapStatus, in map partition order. Null: the
 * driver knows no location for it (lost with its node), so the read fails. */
export interface ShuffleInput {
  readonly shuffle: number;
  readonly maps: readonly (MapStatus | null)[];
}

/** A task, as the driver names it and the executor runs it. */
export interface TaskId {
  /** Spark's task id: unique in the query. */
  readonly tid: number;
  readonly stage: number;
  readonly stageAttempt: number;
  readonly partition: number;
  /** 0, then 1, 2... each time the partition runs again in this stage attempt. */
  readonly attempt: number;
}

export type Command =
  | (TaskId & {
      readonly type: "LaunchTask"; readonly executor: number;
      /** The task slot (and so the core) it runs in. */
      readonly slot: number;
      readonly shuffles: readonly ShuffleInput[];
      /** The broadcasts its joins read. */
      readonly broadcasts: readonly number[];
    })
  /** No stage left reads this shuffle: delete its files. */
  | { readonly type: "RemoveShuffle"; readonly executor: number; readonly shuffle: number }
  /** No stage left reads this broadcast: free the executor's copy. */
  | { readonly type: "RemoveBroadcast"; readonly executor: number; readonly broadcast: number };

/** What a task did, for the driver's Stages and Tasks pages. */
export interface TaskMetrics {
  readonly start: number;
  readonly end: number;
  readonly time: Readonly<Record<Category, number>>;
  /** [kind, start, end] on its core's timeline: a category, or "wait" for the IO channel. */
  readonly segments: readonly (readonly [Category | "wait", number, number])[];
  readonly inputBytes: number;
  readonly inputRows: number;
  readonly shuffleReadBytes: number;
  readonly shuffleReadRows: number;
  readonly shuffleWriteBytes: number;
  readonly shuffleWriteRows: number;
  /** Bytes a Sort spilled to disk. */
  readonly spillBytes: number;
  readonly peakMemory: number;
  /** Rows each operator gave out, by operator id, with notes on what it did. */
  readonly ops: readonly { readonly op: number; readonly rows: number; readonly notes: readonly string[] }[];
  /** A broadcast this task fetched and built on its executor. */
  readonly fetched: readonly number[];
}

/** A shuffle file a task could not read. */
export interface FetchFailure {
  readonly shuffle: number;
  readonly map: number;
  /** The node it was on, or null if the driver had no location for it. */
  readonly node: number | null;
}

export type ExecutorEvent =
  | (TaskId & {
      readonly type: "TaskFinished"; readonly executor: number;
      /** A map task's shuffle file, or null for a result task. */
      readonly mapStatus: MapStatus | null;
      /** A result task's rows, or null for a map task. */
      readonly rows: readonly Row[] | null;
      readonly metrics: TaskMetrics;
    })
  | (TaskId & {
      readonly type: "TaskFailed"; readonly executor: number;
      readonly error: string;
      readonly name: string;
      /** A shuffle file it read is gone: FetchFailed. */
      readonly fetchFailed: FetchFailure | null;
      readonly metrics: TaskMetrics | null;
    });

/** From the infrastructure: a node is gone, with its executor, its running tasks and
 * its shuffle files. */
export interface ExecutorLost {
  readonly type: "ExecutorLost";
  readonly executor: number;
}

export type DriverEvent = ExecutorEvent | ExecutorLost;
export type Message = Command | DriverEvent;

/** A shuffle file a task reads is gone (its node is) or has no known location: the
 * executor's view of a lost block. The task fails with FetchFailed. */
export class FetchFailedError extends Error {
  readonly failure: FetchFailure;
  constructor(failure: FetchFailure) {
    super(failure.node === null
      ? `missing an output location for shuffle ${failure.shuffle}, map ${failure.map}`
      : `shuffle ${failure.shuffle}, map ${failure.map} was on node ${failure.node}, which is gone`);
    this.name = "FetchFailed";
    this.failure = failure;
  }
}

/** "task 3.0 in stage 1.0 (TID 7)", as Spark names a task. */
export function taskName(t: TaskId): string {
  return `task ${t.partition}.${t.attempt} in stage ${t.stage}.${t.stageAttempt} (TID ${t.tid})`;
}

/** One line about a message, for the log. */
export function describeMessage(m: Message): string {
  const n = (x: number) => x.toLocaleString("en");
  switch (m.type) {
    case "LaunchTask": {
      const maps = m.shuffles.reduce((a, s) => a + s.maps.length, 0);
      const extra = [maps ? `the locations of ${n(maps)} shuffle file(s)` : "", m.broadcasts.length ? `broadcast ${m.broadcasts.join(", ")}` : ""].filter(Boolean);
      return `LaunchTask ${taskName(m)} in slot ${m.slot}${extra.length ? `, with ${extra.join(" and ")}` : ""}`;
    }
    case "RemoveShuffle": return `RemoveShuffle ${m.shuffle}`;
    case "RemoveBroadcast": return `RemoveBroadcast ${m.broadcast}`;
    case "TaskFinished": return `TaskFinished ${taskName(m)}: ${m.mapStatus ? `MapStatus (${n(m.mapStatus.bytes.reduce((a, b) => a + b, 0))} bytes in ${m.mapStatus.bytes.filter((b) => b > 0).length} reduce partition(s))` : `${n(m.rows?.length ?? 0)} row(s)`}`;
    case "TaskFailed": return `TaskFailed ${taskName(m)}: ${m.fetchFailed ? `FetchFailed (${m.error})` : m.error}`;
    case "ExecutorLost": return `ExecutorLost: executor ${m.executor}`;
    default: {
      const never: never = m;
      return never;
    }
  }
}
