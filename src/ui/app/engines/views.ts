// What the workbench's shared views ask of an engine's views. The shared views (the
// step banner, Steps, the timeline, Trace, Plan, Result) know only the state's shared
// fields (execution/engine.ts: ExecState); an engine says how its units look, what
// its nodes and units are called, and which tabs show its own objects.

import type { VNode } from "preact";
import type { Json, Query, SubTab } from "../store.ts";

/** How a unit of work looks on the timeline and in the trace. */
export interface UnitLook {
  /** CSS classes (its color, failed, ran again). */
  readonly cls: string;
  /** The text inside its rectangle on the full timeline. */
  readonly text: string;
  readonly title: string;
  /** It is the thing selected in the engine's views. */
  readonly on: boolean;
  /** Selects it in the engine's views. */
  readonly select: () => void;
}

/** A group of units (kostka: a pipeline), as a row of the trace. */
export interface UnitGroup {
  readonly group: string;
  readonly label: string;
  readonly value: string;
}

export interface EngineViews {
  /** What runs on a node, as the views call it ("worker"), and its short name ("W1"). */
  readonly nodeNoun: string;
  nodeName(node: number): string;
  /** What a unit is, in the engine's words ("split"). */
  readonly unitNoun: string;
  /** How units are colored, for the views' notes ("colored by pipeline"). */
  readonly unitColors: string;
  unit(u: Json, q: Query): UnitLook;
  /** The trace's rows of units, in order. */
  groups(state: Json): UnitGroup[];
  /** What is running now, after the Finish button ("P0, P1"). */
  running(state: Json): string;
  /** What the Steps view says about the engine's steps and messages. */
  readonly stepsNote: VNode;
  /** Tabs of the engine's own objects, shown after Plan. */
  readonly tabs: readonly { readonly key: SubTab; readonly label: string; view(p: { q: Query; state: Json }): VNode }[];
}
