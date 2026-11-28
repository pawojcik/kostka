// The Spark-style engine's Plan by stage tab: the Spark plan cut into stages, as
// columns from the scans (left) to the result (right). Each stage is a card: its
// kind, job, tasks and status, and its operators in one WholeStageCodegen box (they
// run in one task, batch by batch), top first. The edges between stages are the
// exchanges: a stage's Exchange (its shuffle) leads to the stages that read it, and a
// broadcast job's BroadcastExchange to the stages whose joins read it. Clicking an
// operator shows what it does and the rows it gave out.

import { update, type Json, type Query } from "../../store.ts";
import { bytes, n, pc } from "../../format.ts";
import { count } from "./views.tsx";

const OP_HELP: Record<string, string> = {
  FileScan: "Reads one data file per task: only the columns the query uses.",
  Filter: "Keeps the rows for which the condition is TRUE.",
  Project: "Computes its output columns.",
  HashAggregate: "partial: folds each task's rows into groups; final: merges the partial groups of its partition, after an Exchange by the keys.",
  Exchange: "A shuffle: each map task writes one shuffle file with its rows grouped by reduce partition; each task of the next stage reads its partition from every file. The plan is cut into stages here.",
  BroadcastExchange: "Its child runs as a job of its own; the driver collects the rows, and every executor fetches them once.",
  BroadcastHashJoin: "Probes a hash table built from a broadcast: built once per executor, shared by its tasks.",
  BroadcastNestedLoopJoin: "A join without equality keys: every row checked against every broadcast row.",
  SortMergeJoin: "Both sides are shuffled by the join keys into the same partitions and sorted: a task walks its two sorted partitions side by side.",
  Sort: "Sorts its partition; spills sorted runs to disk when it outgrows the task's memory, and merges them.",
  TakeOrderedAndProject: "ORDER BY ... LIMIT: the best n rows of each task, then the best n of those in one task.",
  LocalLimit: "Keeps n rows of each partition.",
  GlobalLimit: "Keeps n rows of the one partition.",
};

/** A stage's column: 0 if it reads nothing, else one past what it reads. */
function levels(state: Json): Map<number, number> {
  const out = new Map<number, number>();
  const resultOf = (job: number) => state.jobs[job].stages.at(-1);
  const level = (id: number): number => {
    const have = out.get(id);
    if (have !== undefined) return have;
    const s = state.stages[id];
    const before = [...s.parents, ...s.broadcasts.map((b: number) => resultOf(state.jobs.find((j: Json) => j.broadcast === b).id))];
    const l = before.length ? 1 + Math.max(...before.map(level)) : 0;
    out.set(id, l);
    return l;
  };
  for (const s of state.stages) level(s.id);
  return out;
}

const STATUS: Record<string, string> = { waiting: "", running: "run", done: "ok" };

function OpTree({ q, state, stage, id, depth }: { q: Query; state: Json; stage: Json; id: number; depth: number }) {
  const o = state.ops[id];
  const sel = q.sel?.kind === "sop" && q.sel.id === id;
  const join = state.joins.find((j: Json) => j.op === id);
  // inside the stage: its children in the same stage; an exchange child is an edge
  const kids: number[] = o.children.filter((c: number) => state.ops[c].stage === stage.id && !state.ops[c].exchange);
  const edges: Json[] = o.children.map((c: number) => state.ops[c]).filter((c: Json) => c.exchange);
  return (
    <>
      <div class={`sop ${sel ? "sel" : ""}`} style={`padding-left:${6 + depth * 14}px`} onClick={(e) => { e.stopPropagation(); q.sel = sel ? null : { kind: "sop", id }; update(); }}>
        <b>{o.name}</b> <span class="faint">{o.args}</span>
        {state.units_run > 0 && <span class="rows">{n(o.rows)} rows</span>}
      </div>
      {sel && (
        <div class="sopinfo note" style={`margin-left:${6 + depth * 14}px`}>
          {OP_HELP[o.name] ?? ""}
          {join && <> Build side ≈ {join.buildBytes === null ? "—" : bytes(join.buildBytes)}, threshold {join.threshold < 0 ? "off" : bytes(join.threshold)}: {join.strategy === "BroadcastHashJoin" ? "at most the threshold, so it is broadcast" : join.strategy === "SortMergeJoin" ? (join.threshold < 0 ? "broadcasts are off, so both sides are shuffled" : "over the threshold, so both sides are shuffled") : "no equality keys, so the right side is broadcast"}.</>}
        </div>
      )}
      {edges.map((c) => {
        const from = c.exchange === "shuffle" ? state.stages.find((s: Json) => s.writes === c.shuffle) : null;
        const job = c.exchange === "broadcast" ? state.jobs.find((j: Json) => j.broadcast === c.broadcast) : null;
        return (
          <div class="sedge in" style={`padding-left:${6 + (depth + 1) * 14}px`}>
            ⇠ {c.exchange === "shuffle" ? <>reads <b>Exchange</b> {c.args} from <button class="link" onClick={(e) => { e.stopPropagation(); q.sel = { kind: "stage", id: from.id }; update(); }}>stage {from.id}</button></> : <>reads <b>BroadcastExchange</b> {c.broadcast} from job {job.id}</>}
          </div>
        );
      })}
      {kids.map((c) => <OpTree q={q} state={state} stage={stage} id={c} depth={depth + 1} />)}
    </>
  );
}

function StageCard({ q, state, s }: { q: Query; state: Json; s: Json }) {
  const sel = q.sel?.kind === "stage" && q.sel.id === s.id;
  const ex = s.exchange !== null ? state.ops[s.exchange] : null;
  const job = state.jobs[s.job];
  const readers = s.writes !== null ? state.stages.filter((x: Json) => x.reads.includes(s.writes)).map((x: Json) => x.id) : [];
  const bcReaders = job.broadcast !== null && job.stages.at(-1) === s.id ? state.stages.filter((x: Json) => x.broadcasts.includes(job.broadcast)).map((x: Json) => x.id) : [];
  return (
    <div class={`scard ${sel ? "sel" : ""}`} data-tour={`stage-${s.id}`} onClick={() => { q.sel = sel ? null : { kind: "stage", id: s.id }; update(); }}>
      <div class="row"><i class={`sw ${pc(s.id)}`} /><b class="grow">Stage {s.id}</b><span class={`chip ${STATUS[s.status] ?? ""}`}>{s.status}{s.attempt > 0 ? ` · attempt ${s.attempt}` : ""}</span></div>
      <span class="note">{s.kind === "map" ? "ShuffleMapStage" : "ResultStage"} · job {s.job}{job.broadcast !== null ? ` (broadcast ${job.broadcast})` : ""} · {count(s.partitions, "task")}{s.tasks.done ? ` · ${n(s.tasks.done)} done` : ""}</span>
      <div class="wscg" title="WholeStageCodegen: these operators run in one task, batch by batch">
        <span class="wscg-l">WholeStageCodegen *({s.id + 1})</span>
        <OpTree q={q} state={state} stage={s} id={s.top} depth={0} />
      </div>
      {ex && <div class="sedge out">⇢ <b>Exchange</b> {ex.args} (shuffle {s.writes}){readers.length ? ` → stage ${readers.join(", ")}` : ""}</div>}
      {bcReaders.length > 0 && <div class="sedge out">⇢ <b>BroadcastExchange</b> {job.broadcast} → stage {bcReaders.join(", ")}</div>}
      {s.kind === "result" && job.broadcast === null && <div class="sedge out">⇢ rows to the driver: the result</div>}
    </div>
  );
}

export function StagePlanView({ q, state }: { q: Query; state: Json }) {
  const lv = levels(state);
  const cols = Math.max(0, ...lv.values()) + 1;
  return (
    <div class="stack" data-tour="spark-plan">
      <p class="note">The Spark plan, cut into <b>stages</b> at every Exchange: data flows left to right. A stage's operators run in one task per partition (a WholeStageCodegen box); its tasks start only when every stage it reads is done. A <b>job</b> runs the query's result; each BroadcastExchange is a job of its own, run first. Click a stage or an operator.</p>
      <div class="sdag">
        {Array.from({ length: cols }, (_, c) => (
          <div class="scol">{state.stages.filter((s: Json) => lv.get(s.id) === c).map((s: Json) => <StageCard q={q} state={state} s={s} />)}</div>
        ))}
      </div>
      <details><summary>The plan as Spark's explain prints it</summary><pre class="mono">{q.plans?.explain}</pre></details>
    </div>
  );
}
