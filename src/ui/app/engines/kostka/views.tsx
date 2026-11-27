// kostka's views, as the shared views see them (../views.ts): a node's worker is
// "W1", a unit is a split colored by its pipeline, the trace has a row per pipeline,
// and the Program, Query state and Workers tabs show kostka's own objects.

import { update, type Json, type Query } from "../../store.ts";
import { ms, n, pc } from "../../format.ts";
import type { EngineViews } from "../views.ts";
import { ProgramView } from "./program.tsx";
import { QueryStateView } from "./state.tsx";
import { WorkersView } from "./workers.tsx";

export const KOSTKA_VIEWS: EngineViews = {
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
  tabs: [
    { key: "program", label: "Program", view: ({ q, state }) => <ProgramView q={q} state={state} /> },
    { key: "state", label: "Query state", view: ({ q, state }) => <QueryStateView q={q} state={state} /> },
    { key: "workers", label: "Workers", view: ({ q, state }) => <WorkersView q={q} state={state} /> },
  ],
};
