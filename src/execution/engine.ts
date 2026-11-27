// The seam between kostka's shared layers and its engines: what an engine is, what
// running a query on one looks like (an Execution), and what every engine's state
// has in common (ExecState).
//
// An engine takes the optimized plan and plans it in its own way (kostka cuts it
// into pipelines and buffers; ADR 0002 adds one with stages and tasks), then runs it
// on the shared simulated cluster (cluster/). Nothing outside src/engines/ knows any
// engine's objects: the API, the Session and the workbench's shared views use only
// this file's types; views of an engine's own objects pick the engine by
// `ExecState.engine`.
//
// An execution is stepped from outside: nothing runs until `step()` is called. Steps
// are async because a unit of work may first load a data file. The history is a list
// of steps (a scheduler's decision, or a unit run on a node), each with its messages;
// `state(at)` shows the whole query as it was right after step `at`.

import type { Row } from "../core/types.ts";
import type { PlanNode } from "../sql/plan.ts";
import type { TableReader } from "../storage/table-reader.ts";
import type { ClusterConfig } from "../cluster/config.ts";
import type { NodeFrame, SegmentKind } from "../cluster/node.ts";
import type { LoggedMessage, Step } from "../cluster/loop.ts";

export type { LoggedMessage };
/** A step of an execution's history. */
export type ExecEvent = Step;

/** A kind of step: who takes it, how the workbench names it, and what it means. */
export interface StepKindInfo {
  readonly kind: string;
  /** "scheduler": a decision; "node": a unit of work ran; "failure": something broke. */
  readonly by: "scheduler" | "node" | "failure";
  readonly label: string;
  readonly doc: string;
  /** A step that is bad news (shown in red). */
  readonly bad: boolean;
}

/** What an engine is given to start a query. */
export interface EngineStart {
  readonly optimized: PlanNode;
  readonly reader: TableReader;
  readonly cluster: ClusterConfig;
  /** The engine's own settings, unchecked: the engine checks them. */
  readonly settings: Readonly<Record<string, unknown>>;
}

export interface Engine {
  /** Its name in options ("kostka"). */
  readonly name: string;
  /** How the workbench names it ("kostka (pipelines)"). */
  readonly label: string;
  readonly steps: readonly StepKindInfo[];
  /** The kinds of step `Session.next` stops at by default: a unit of work ran. */
  readonly unitSteps: readonly string[];
  /** Plans the optimized plan and starts an execution; nothing runs yet. Throws a
   * KostkaError if the settings are wrong. */
  start(q: EngineStart): Execution;
}

/** A unit of work on a core's timeline, as every engine shows it. */
export interface UnitFrame {
  readonly id: number;
  readonly node: number;
  readonly core: number;
  readonly start_ms: number;
  readonly end_ms: number;
  readonly label: string;
  /** What it belongs to, for grouping in the trace ("P1"). */
  readonly group: string;
  /** Why it failed, if it did. */
  readonly failed: string | null;
  /** Where its time went on the core's timeline, in ms: CPU, IO, and waits for the
   * node's IO channel. */
  readonly segments: readonly (readonly [kind: SegmentKind, start_ms: number, end_ms: number])[];
}

/** What `state(at)` returns: the fields every engine has, and the engine's own. */
export interface ExecState {
  readonly engine: string;
  /** The step shown, and how many there are. */
  readonly at: number;
  readonly events: number;
  readonly event: ExecEvent;
  readonly done: boolean;
  readonly failed: { readonly unit: string; readonly error: string } | null;
  readonly elapsed_ms: number;
  /** What happens next: a unit on a node, or the scheduler. */
  readonly next: { readonly node: number | null; readonly kind: string; readonly label: string; readonly refs: Readonly<Record<string, number>> } | null;
  readonly cluster: ClusterConfig;
  /** Each node right after the step, with the engine's view of it. */
  readonly nodes: readonly NodeFrame[];
  readonly units: readonly UnitFrame[];
  /** Result rows received so far. */
  readonly result_rows: number;
  /** The engine's own objects (kostka: pipelines, splits, buffers, filters). */
  readonly [key: string]: unknown;
}

export interface Execution {
  readonly engine: string;
  readonly done: boolean;
  /** Goes on to the next step. Returns false once there is none. */
  step(): Promise<boolean>;
  /** Steps until each node that can ran one unit: the units that run side by side. */
  wave(): Promise<number>;
  /** Steps until the work running now (or, if none, the work ready next) is done. */
  runConcurrent(onProgress?: () => void): Promise<void>;
  run(onProgress?: () => void): Promise<Row[]>;
  /** A failure, now: node `n` stops, with everything on it. */
  stopNode(n: number): void;
  /** The result rows; an error before the query is done. */
  result(): Row[];
  /** The result rows received so far. */
  rows(): readonly Row[];
  /** The state right after step `at` (default: the last one). JSON-friendly. */
  state(at?: number): ExecState;
  /** The engine's physical plan, as text. */
  explain(): string;
  /** The steps so far. The first (the query started) is there from the start, so
   * `state()` always has a step to show. */
  readonly events: readonly ExecEvent[];
  /** Units done and in all, for progress bars. */
  progress(): { done: number; total: number };
  /** Simulated ns so far: when the busiest node is free again. */
  readonly elapsedNs: number;
}
