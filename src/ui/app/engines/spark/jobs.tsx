// The Spark-style engine's Jobs · Stages · Tasks tab, after Spark's web UI: the jobs
// (status, stages done out of all), the stages (attempt, tasks done, running and
// failed out of all, bytes read from storage, shuffle read and write, spill, time),
// and the tasks of the stage chosen (partition, attempt, executor, slot, status,
// time, spill, the error). All as they were right after the step shown.

import { update, type Json, type Query } from "../../store.ts";
import { bytes, ms, n, pc } from "../../format.ts";
import { taskName } from "./views.tsx";

const JOB: Record<string, string> = { waiting: "", running: "run", succeeded: "ok", failed: "bad" };
const STAGE: Record<string, string> = { waiting: "", running: "run", done: "ok" };
const TASK: Record<string, string> = { running: "run", success: "ok", failed: "bad", lost: "skip" };

/** The stage the Tasks page shows: the one chosen, else the latest running one. */
function chosen(q: Query, state: Json): Json | null {
  if (q.sel?.kind === "stage") return state.stages[Number(q.sel.id)] ?? null;
  if (q.sel?.kind === "task") {
    const t = state.tasks.find((x: Json) => x.tid === q.sel!.id);
    if (t) return state.stages[t.stage];
  }
  const running = state.stages.filter((s: Json) => s.status === "running");
  return running.at(-1) ?? [...state.stages].reverse().find((s: Json) => s.tasks.done > 0) ?? null;
}

export function JobsView({ q, state }: { q: Query; state: Json }) {
  const st = chosen(q, state);
  const tasks: Json[] = st ? state.tasks.filter((t: Json) => t.stage === st.id) : [];
  return (
    <div class="stack" data-tour="spark-jobs">
      <section class="panel stack">
        <h4>Jobs</h4>
        <table class="data">
          <thead><tr><th>job</th><th>what</th><th>status</th><th class="n">stages done</th><th>after job</th></tr></thead>
          <tbody>{state.jobs.map((j: Json) => (
            <tr><td>{j.id}</td><td>{j.broadcast === null ? "the query's result" : `broadcast ${j.broadcast}`}</td><td><span class={`chip ${JOB[j.status] ?? ""}`}>{j.status}</span></td>
              <td class="n">{n(j.stages_done)} / {n(j.stages.length)}</td><td>{j.needs.length ? j.needs.join(", ") : "—"}</td></tr>
          ))}</tbody>
        </table>
      </section>
      <section class="panel stack" data-tour="spark-stages">
        <h4>Stages</h4>
        <div class="scroll">
          <table class="data">
            <thead><tr><th>stage</th><th>job</th><th>kind</th><th>status</th><th class="n">attempt</th><th class="n">tasks done / running / failed of all</th><th class="n">input</th><th class="n">shuffle read</th><th class="n">shuffle write</th><th class="n">spill</th><th class="n">time</th></tr></thead>
            <tbody>{state.stages.map((s: Json) => (
              <tr class={`click ${st?.id === s.id ? "on" : ""}`} onClick={() => { q.sel = { kind: "stage", id: s.id }; update(); }} title={s.why}>
                <td><i class={`sw ${pc(s.id)}`} /> {s.id}</td><td>{s.job}</td><td>{s.kind === "map" ? "ShuffleMap" : "Result"}</td>
                <td><span class={`chip ${STAGE[s.status] ?? ""}`}>{s.status}</span>{s.status === "waiting" && s.attempt >= 0 ? <span class="faint"> {s.why}</span> : null}</td>
                <td class="n">{s.attempt < 0 ? "—" : s.attempt}</td>
                <td class="n">{n(s.tasks.done)} / {n(s.tasks.running)} / {n(s.tasks.failed)} of {n(s.tasks.total)}</td>
                <td class="n">{s.input_bytes ? bytes(s.input_bytes) : "—"}</td><td class="n">{s.shuffle_read_bytes ? bytes(s.shuffle_read_bytes) : "—"}</td>
                <td class="n">{s.shuffle_write_bytes ? bytes(s.shuffle_write_bytes) : "—"}</td><td class="n">{s.spill_bytes ? bytes(s.spill_bytes) : "—"}</td>
                <td class="n">{s.start_ms !== null && s.end_ms !== null ? ms(s.end_ms - s.start_ms) : s.start_ms !== null ? "running" : "—"}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        <p class="note">A stage's attempt goes up each time it is resubmitted, for the partitions whose output is missing. Input is read from table storage; shuffle read and write are the shuffle files between stages. Click a stage to see its tasks.</p>
      </section>
      <section class="panel stack" data-tour="spark-tasks">
        <h4>Tasks{st ? ` of stage ${st.id}` : ""}</h4>
        {!st || !tasks.length ? <p class="empty">No task of {st ? `stage ${st.id}` : "any stage"} launched yet.</p> : (
          <div class="scroll">
            <table class="data">
              <thead><tr><th class="n">TID</th><th class="n">partition</th><th class="n">attempt</th><th class="n">stage attempt</th><th>executor</th><th class="n">slot</th><th>status</th><th class="n">time</th><th class="n">spill</th><th>error</th></tr></thead>
              <tbody>{tasks.map((t) => (
                <tr class={`click ${q.sel?.kind === "task" && q.sel.id === t.tid ? "on" : ""}`} title={taskName(t)} onClick={() => { q.sel = { kind: "task", id: t.tid }; update(); }}>
                  <td class="n">{t.tid}</td><td class="n">{t.partition}</td><td class="n">{t.attempt}</td><td class="n">{t.stage_attempt}</td><td>E{t.executor}</td><td class="n">{t.slot}</td>
                  <td><span class={`chip ${TASK[t.status] ?? ""}`}>{t.status === "running" && t.ran ? "reporting" : t.status === "running" ? "launched" : t.status}</span>{t.fetch_failed ? <span class="chip bad">FetchFailed</span> : null}</td>
                  <td class="n">{t.start_ms !== null ? ms(t.end_ms - t.start_ms) : "—"}</td><td class="n">{t.spill_bytes ? bytes(t.spill_bytes) : "—"}</td>
                  <td class="mono small">{t.error ?? ""}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <p class="note">launched: in its slot, waiting for its core; reporting: it ran, and its TaskFinished is on the way to the driver; lost: its node was gone first (it runs again elsewhere, and this is not counted as a failure).</p>
      </section>
    </div>
  );
}
