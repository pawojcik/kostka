// The Spark-style engine's views, as the shared views see them (../views.ts): a node's
// executor is "E1", a unit is a task colored by its stage, the trace has a row per
// stage, and the Plan by stage, Jobs · Stages · Tasks and Executors tabs show its own
// objects.

import { update, type Json, type Query } from "../../store.ts";
import { ms, n, pc } from "../../format.ts";
import type { EngineViews } from "../views.ts";
import { StagePlanView } from "./plan.tsx";
import { JobsView } from "./jobs.tsx";
import { ExecutorsView } from "./executors.tsx";

/** Spark's operators that run plan node `node`, top first. */
function opsOf(state: Json, node: number): Json[] {
  return (state?.ops ?? []).filter((o: Json) => o.node === node);
}

/** "task 3.0 in stage 1.0 (TID 7)", as Spark names a task. */
export const taskName = (t: Json) => `task ${t.partition}.${t.attempt} in stage ${t.stage}.${t.stage_attempt} (TID ${t.tid})`;

export const SPARK_VIEWS: EngineViews = {
  schedulerNoun: "driver",
  nodeNoun: "executor",
  nodeName: (node) => `E${node}`,
  unitNoun: "task",
  unitColors: "colored by stage",
  unit: (u: Json, q: Query) => ({
    cls: `${pc(u.stage)} ${u.failed ? "failed" : ""} ${u.attempt > 0 || u.stage_attempt > 0 ? "again" : ""}`,
    text: `S${u.stage}`,
    title: `${taskName(u)} on E${u.node}, slot ${u.core}${u.failed ? `\nfailed: ${u.failed}` : ""}\n${ms(u.start_ms)} → ${ms(u.end_ms)}`,
    on: (q.sel?.kind === "task" && q.sel.id === u.tid) || (q.sel?.kind === "stage" && q.sel.id === u.stage),
    select: () => { q.sel = { kind: "stage", id: u.stage }; update(); },
  }),
  groups: (state: Json) => state.stages.map((s: Json) => ({
    group: `S${s.id}`,
    label: `Stage ${s.id} ${s.ops.map((id: number) => state.ops[id]?.name).join(" ← ")}`,
    value: s.end_ms !== null && s.start_ms !== null ? ms(s.end_ms - s.start_ms) : s.status,
  })),
  running: (state: Json) => state.stages.filter((s: Json) => s.status === "running").map((s: Json) => `stage ${s.id}`).join(", "),
  stepsNote: <p class="note">The driver and the executors talk only by messages: the driver sends <b>commands</b> (LaunchTask, RemoveShuffle, RemoveBroadcast), the executors answer with <b>events</b> (TaskFinished, TaskFailed); the infrastructure says when a node is gone (ExecutorLost). The driver's steps are named like Spark's listener events: JobStart, StageSubmitted, TaskStart, TaskEnd, StageCompleted, JobEnd, and FetchFailed and StageResubmitted when shuffle files are lost. A message a gone executor sent that never arrived is shown struck through.</p>,
  nodeHelp: {
    Scan: "Spark-style: a FileScan, one task per data file.",
    Aggregate: "Spark-style: HashAggregate(partial) in each task, an Exchange by the keys (one partition without keys), then HashAggregate(final).",
    Join: "Spark-style: a BroadcastHashJoin when the build side is estimated under the broadcast threshold, else a SortMergeJoin with both sides shuffled by the keys and sorted; a join without keys is a BroadcastNestedLoopJoin.",
    Sort: "Spark-style: an Exchange to one partition, then a Sort, spilling to disk if it outgrows the task's memory.",
    TopN: "Spark-style: TakeOrderedAndProject, the best n in each task, then the best n of those in one.",
    Limit: "Spark-style: LocalLimit in each task, then GlobalLimit in one.",
  },
  nodeOps: (state, node) => opsOf(state, node).map((o) => ({
    label: `${o.name}${o.stage !== null ? ` (stage ${o.stage})` : ""}`,
    go: (q: Query) => { q.sub = "spark-plan"; q.sel = { kind: "sop", id: o.id }; },
  })),
  nodeRows: (state, node) => {
    const ops = opsOf(state, node);
    if (!ops.length || !state?.units_run) return null;
    // the node's output is its topmost operator's (the lowest id: ids are preorder)
    return ops.reduce((a: Json, b: Json) => (b.id < a.id ? b : a)).rows;
  },
  controlsNote: <p class="note"><b>Compile</b> analyzes the SQL, runs the chosen rules, plans the Spark plan and cuts it into jobs and stages, pinned to the tables' current versions. <b>Step</b> goes to the next step: the driver starting a job, submitting a stage, launching a task or handling its end; an executor running a task; a stage or job done. It runs the simulation when that step has not happened yet. <b>Next task</b> goes on to the next task an executor ran. <b>Wave</b> runs until each core that can ran one task. <b>Finish</b> runs until the stages running now are done. Every step, with its messages, stays in the history.</p>,
  tabs: [
    { key: "spark-plan", label: "Plan by stage", view: ({ q, state }) => <StagePlanView q={q} state={state} /> },
    { key: "spark-jobs", label: "Jobs · Stages · Tasks", view: ({ q, state }) => <JobsView q={q} state={state} /> },
    { key: "spark-executors", label: "Executors", view: ({ q, state }) => <ExecutorsView q={q} state={state} /> },
  ],
};

/** A count with its noun ("3 tasks"). */
export const count = (k: number, noun: string) => `${n(k)} ${noun}${k === 1 ? "" : "s"}`;
