// kostka's views, as the shared views see them (../views.ts): a node's worker is
// "W1", a unit is a split colored by its pipeline, the trace has a row per pipeline,
// and the Program, Query state and Workers tabs show kostka's own objects.

import { update, type Json, type Query } from "../../store.ts";
import { ms, n, pc } from "../../format.ts";
import type { EngineViews } from "../views.ts";
import { ProgramView } from "./program.tsx";
import { QueryStateView } from "./state.tsx";
import { WorkersView } from "./workers.tsx";

/** kostka's operators of a plan node, with their pipeline. */
function opsOfNode(state: Json, node: number): Json[] {
  return (state?.pipelines ?? []).flatMap((p: Json) => p.ops.filter((o: Json) => o.node === node).map((o: Json) => ({ ...o, pipeline: p.index })));
}

export const KOSTKA_VIEWS: EngineViews = {
  schedulerNoun: "coordinator",
  nodeNoun: "worker",
  nodeName: (node) => `W${node}`,
  unitNoun: "split",
  unitColors: "colored by pipeline",
  unit: (u: Json, q: Query) => ({
    cls: `${pc(u.pipeline)} ${u.failed ? "failed" : ""} ${u.attempt > 1 ? "again" : ""}`,
    text: `P${u.pipeline}`,
    title: `P${u.pipeline} on W${u.worker}: split ${u.split}${u.attempt > 1 ? ` (attempt ${u.attempt})` : ""} (${u.label})${u.failed ? `\nfailed: ${u.failed}` : ""}\n${ms(u.start_ms)} → ${ms(u.end_ms)}\n${u.ops.map((o: Json, i: number) => `${i}: ${n(o.rows_in)} → ${n(o.rows_out)} rows`).join("\n")}`,
    on: q.sel?.kind === "split" && q.sel.id === u.split,
    select: () => { q.sel = { kind: "split", id: u.split }; update(); },
  }),
  groups: (state: Json) => state.pipelines.map((p: Json) => ({
    group: `P${p.index}`,
    label: `P${p.index} ${p.ops.map((o: Json) => o.label.split("(")[0]).join(" → ")}`,
    value: p.end_ms === null ? p.status : ms(p.end_ms - p.start_ms),
  })),
  running: (state: Json) => state.pipelines.filter((p: Json) => p.status === "running").map((p: Json) => `P${p.index}`).join(", "),
  stepsNote: <p class="note">The coordinator and the workers talk only by messages: the coordinator sends <b>commands</b> (AddSplits, ReleaseBuffer), the workers answer with <b>events</b> (SplitDone, SplitFailed); the infrastructure says when a worker is gone (WorkerGone). Every step groups the messages of one decision of the coordinator, or of one split a worker ran; a message a gone worker sent that never arrived is shown struck through. The views show the query right after the step chosen here or with the history slider.</p>,
  nodeHelp: {
    Scan: "kostka: one split per data file.",
    Aggregate: "kostka: in two phases, partial per split, then merged per partition.",
    Join: "kostka: a hash join; the build side is broadcast or shuffled by key.",
    Sort: "kostka: each split sorts what it got, then one split merges the sorted runs.",
    TopN: "kostka: each split keeps its best n, then one split merges them.",
  },
  nodeOps: (state, node) => opsOfNode(state, node).map((o) => ({ label: `${o.id} ${o.label}`, go: (q: Query) => { q.sub = "program"; q.sel = { kind: "op", id: o.id }; } })),
  // Rows a node produced: out of the last of its operators that is not a sink.
  nodeRows: (state, node) => {
    const ops = opsOfNode(state, node).filter((o) => o.role !== "sink");
    if (!ops.length || !state?.units_run) return null;
    return ops[ops.length - 1].metrics.rows_out;
  },
  controlsNote: <p class="note"><b>Compile</b> analyzes the SQL, runs the chosen rules and plans the pipelines, pinned to the tables' current versions. <b>Step</b> goes to the next step: the coordinator making a pipeline's splits, assigning splits, handling a SplitDone or a worker gone; a worker processing a split; a pipeline done. It runs the simulation when that step has not happened yet. <b>Next split</b> goes on to the next split processed. <b>Wave</b> runs until each worker ran one split. <b>Finish</b> runs until the pipelines running now are done. Every step, with its messages, stays in the history.</p>,
  tabs: [
    { key: "program", label: "Program", view: ({ q, state }) => <ProgramView q={q} state={state} /> },
    { key: "state", label: "Query state", view: ({ q, state }) => <QueryStateView q={q} state={state} /> },
    { key: "workers", label: "Workers", view: ({ q, state }) => <WorkersView q={q} state={state} /> },
  ],
};
