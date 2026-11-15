// The Execution API: how a physical plan is run. The physical plan says what to
// compute (pipelines, output buffers, operators); an Execution runs it: a coordinator
// and workers (the domain, coordinator/ and worker/) talking by messages over a
// simulated cluster (the infrastructure, sim/).
//
// An execution is stepped from outside: nothing runs until `step()` is called.
// Steps are async because a unit of work may first load a data file.
//
// The history is the message log, grouped into *steps*: the coordinator making a
// pipeline's splits, assigning splits, handling a split's report or a worker gone;
// a worker processing a split; a pipeline done. Each step keeps enough to show the
// whole query as it was right after it (`state(at)`), so the workbench can step and
// go back step by step.

import type { Row } from "../core/types.ts";
import type { StepKind } from "./coordinator/coordinator.ts";

export type EventKind = StepKind;

/** One message of a step: a command or an event, as the log shows it. */
export interface LoggedMessage {
  /** "lost": an event a worker sent that was never delivered (it was gone). */
  readonly dir: "command" | "event" | "lost";
  readonly type: string;
  /** "coordinator", "simulation" or "W<n>". */
  readonly from: string;
  readonly to: string;
  readonly ms: number;
  readonly text: string;
}

/** A step of the history. */
export interface ExecEvent {
  readonly seq: number;
  /** Simulated time, ms. */
  readonly ms: number;
  readonly kind: EventKind;
  readonly text: string;
  readonly worker: number | null;
  readonly pipeline: number | null;
  readonly split: number | null;
  readonly buffer: number | null;
  readonly partition: number | null;
  readonly messages: readonly LoggedMessage[];
}

export interface Execution {
  readonly done: boolean;
  /** Goes on to the next step. Returns false once there is none. */
  step(): Promise<boolean>;
  /** Steps until each worker that can ran one split: the splits that run side by side. */
  wave(): Promise<number>;
  /** Steps until the pipelines running now (or, if none, the ready ones) are done. */
  runConcurrent(onProgress?: () => void): Promise<void>;
  run(onProgress?: () => void): Promise<Row[]>;
  /** A failure, now: worker `w` stops, with everything it holds. */
  stopWorker(w: number): void;
  /** The result rows; an error before the query is done. */
  result(): Row[];
  /** The result rows received so far. */
  rows(): readonly Row[];
  /** Everything the workbench shows, JSON-friendly, right after step `at` (default:
   * the last one). */
  state(at?: number): Record<string, unknown>;
  /** The steps so far. */
  readonly events: readonly ExecEvent[];
  /** Splits done and in all, for progress bars. */
  progress(): { done: number; total: number };
  /** Simulated ns so far: when the busiest worker is free again. */
  readonly elapsedNs: number;
}
