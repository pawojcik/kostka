// The page's state: one object, changed in place, and `update()` to re-render. The
// worker holds the database and the queries; this holds what the page shows.

import type { Catalog } from "../editor.ts";

export type Json = any;

export interface QueryOptions {
  rules: string[];
  cluster: { nodes: number; memoryMB: number; diskMB: number; io: string; batchRows: number; splitRows: number; cpu: "measured" | "model" };
  planner: { shufflePartitions: number | "auto"; joinStrategy: "auto" | "broadcast" | "shuffle"; runtimeFilters: boolean };
}

export type SubTab = "plan" | "program" | "events" | "state" | "workers" | "trace" | "result";

export interface Query {
  key: string;
  name: string;
  sql: string;
  /** The SQL the current plans were compiled from (null: not compiled). */
  compiledSql: string | null;
  options: QueryOptions;
  plans: Json | null;
  /** The state after the latest event. */
  state: Json | null;
  /** The execution's steps so far (the history). */
  events: Json[];
  /** Event kinds the Events view hides. */
  hidden: Set<string>;
  /** What this query is waiting for ("Compiling", "Running"), shown next to its controls. */
  pending: string | null;
  /** The event shown; null follows the latest. */
  at: number | null;
  hist: Map<number, Json>;
  sub: SubTab;
  /** The plan stage shown (0: analyzed, then one per rule); null: the optimized plan. */
  planStep: number | null;
  sel: { kind: "op" | "pipe" | "worker" | "buffer" | "node" | "split"; id: string | number } | null;
  error: string | null;
  progress: { phase?: string; done?: number; total?: number } | null;
}

export interface Activity {
  title: string;
  phase: string;
  done: number;
  total: number;
  status: "running" | "ok" | "bad";
  log: string[];
}

export const S = {
  ready: false,
  storage: "",
  status: "Starting…",
  busy: false,
  /** What the page is waiting for, once a call has taken a moment (null: nothing). */
  waiting: null as string | null,
  failed: false,
  info: null as Json,
  tables: [] as Json[],
  catalog: { tables: {}, functions: [] } as Catalog,
  settings: { fileRows: 8192 },
  tab: "tables" as string,
  selected: null as string | null,
  version: null as number | null,
  detail: null as Json,
  csv: null as Json,
  activity: null as Activity | null,
  queries: [] as Query[],
  draft: { sql: "", example: "" },
  toast: null as string | null,
  nextQuery: 1,
  /** How many times each control was used (Compile, Step, Wave…): tutorials watch these. */
  actions: {} as Record<string, number>,
};

/** Counts a use of a control, for the tutorials. */
export function noteAction(name: string): void {
  S.actions[name] = (S.actions[name] ?? 0) + 1;
}

let rerender: () => void = () => {};
export function setRenderer(fn: () => void): void {
  rerender = fn;
}
export function update(): void {
  rerender();
}

let toastTimer: ReturnType<typeof setTimeout> | undefined;
export function toast(msg: string): void {
  S.toast = msg;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    S.toast = null;
    update();
  }, 6000);
  update();
}

export function defaultOptions(info: Json): QueryOptions {
  const d = info.cluster.defaults;
  return {
    rules: info.rules.map((r: Json) => r.name),
    cluster: { nodes: 2, memoryMB: d.memoryMB, diskMB: d.diskMB, io: d.io, batchRows: d.batchRows, splitRows: d.splitRows, cpu: d.cpu },
    planner: { shufflePartitions: "auto", joinStrategy: "auto", runtimeFilters: true },
  };
}

export function shownState(q: Query): Json {
  return q.at === null ? q.state : (q.hist.get(q.at) ?? q.state);
}
