// Query tabs: the SQL (editable; editing it marks the plans stale), the optimizer
// rules and cluster to compile with, the controls that step the execution, a slider
// back through the steps, and the views of the compiled query.

import { call, callWith } from "../rpc.ts";
import { S, defaultOptions, noteAction, shownState, toast, update, type Json, type Query, type SubTab } from "./store.ts";
import { Editor, DataTable, ProgressBar } from "./widgets.tsx";
import { Spinner } from "./logo.tsx";
import { PlanView } from "./plans.tsx";
import { Timeline } from "./timeline.tsx";
import { TraceView } from "./trace.tsx";
import { EventBanner, EventsView } from "./events.tsx";
import { viewsOf } from "./engines/index.ts";
import { ms, n } from "./format.ts";

const check = (sql: string) => call("check", sql);

export function newQuery(sql: string, name?: string): Query {
  const key = `q${S.nextQuery++}`;
  const q: Query = {
    key, name: name ?? `Query ${key.slice(1)}`, sql, compiledSql: null, options: defaultOptions(S.info), plans: null, state: null,
    events: [], hidden: new Set(), pending: null, at: null, hist: new Map(), sub: "plan", planStep: null, sel: null, error: null, progress: null,
  };
  S.queries.push(q);
  S.tab = key;
  update();
  return q;
}

export function closeQuery(q: Query): void {
  void call("close", q.key);
  S.queries = S.queries.filter((x) => x !== q);
  if (S.tab === q.key) S.tab = S.queries.length ? S.queries[S.queries.length - 1]!.key : "tables";
  update();
}

export function NewQueryTab() {
  const examples: Json[] = S.info?.examples ?? [];
  const ex = examples.find((e) => e.id === S.draft.example);
  // The tables and columns the example needs: a table of the same name from another
  // dataset (SSB and TPC-DS both have customer) counts as missing.
  const lacking: string[] = (ex?.needs ?? []).filter((tc: string) => {
    const [t, c] = tc.split(".");
    return !(S.catalog.tables as Record<string, { name: string }[]>)[t!]?.some((x) => x.name === c);
  }).map((tc: string) => tc.split(".")[0]!);
  const missing = lacking.length > 0;
  const datasets = [...new Set(examples.map((e) => e.dataset as string))];
  return (
    <div class="two">
      <section class="panel stack">
        <h2>New query</h2>
        <label class="field">Example query
          <select class="in" value={S.draft.example} onChange={(e) => {
            const id = (e.target as HTMLSelectElement).value;
            S.draft.example = id;
            S.draft.sql = examples.find((x) => x.id === id)?.sql ?? S.draft.sql;
            update();
          }}>
            <option value="">(write your own)</option>
            {datasets.map((d) => <optgroup label={d}>{examples.filter((e) => e.dataset === d).map((e) => <option value={e.id}>{e.title}</option>)}</optgroup>)}
          </select>
        </label>
        {ex && <p class="explain">{ex.about}</p>}
        {missing && <p class="note">This example runs on the {ex!.dataset === "Sample" ? "users/orders sample" : `${ex!.dataset} tables`}, and {lacking.join(" and ")} {lacking.length > 1 ? "are" : "is"} missing{S.tables.some((t) => lacking.includes(t.name)) ? " or another dataset's table of the same name" : ""}: {ex!.dataset === "Sample" ? "load" : ex!.dataset === "NYC taxi" ? "load" : "generate"} it on the Tables tab first.</p>}
        <Editor doc={S.draft.sql} catalog={S.catalog} check={check} placeholder="select … from …"
          onChange={(t) => { S.draft.sql = t; }} onRun={() => open()} />
        <div class="row"><span class="note grow">Ctrl/⌘ + Enter opens it. Tab completes tables, columns and functions.</span><button class="btn primary" onClick={() => open()}>Open in a new tab</button></div>
      </section>
      <section class="panel stack">
        <h3>Writing queries</h3>
        <div class="note stack">
          <p>kostka runs <b>SELECT</b> queries: <code>FROM</code> with comma joins or <code>[INNER | LEFT | RIGHT | CROSS] JOIN … ON</code>, subqueries in <code>FROM</code>, <code>WITH</code> (CTEs), <code>WHERE</code>, <code>GROUP BY</code>, <code>HAVING</code>, <code>ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]</code>, <code>LIMIT</code>, <code>DISTINCT</code>.</p>
          <p>Expressions: arithmetic, comparisons, <code>AND/OR/NOT</code>, <code>IS [NOT] NULL</code>, <code>[NOT] IN (…)</code>, <code>BETWEEN</code>, <code>LIKE</code>, <code>CASE</code>, <code>CAST(x AS type)</code>, <code>||</code>, and functions (completion lists them with their signatures).</p>
          <p>Types: <code>int</code> (exact up to ±2<sup>53</sup>; overflow is an error), <code>float</code>, <code>string</code>, <code>bool</code>, <code>date</code>, <code>timestamp</code>. Only int → float and date → timestamp convert implicitly; <code>int / int</code> is a float. A string literal compared with a date is read as a date.</p>
          <p>Time travel: <code>FROM t FOR VERSION AS OF 3</code> or <code>FOR TIMESTAMP AS OF '2024-05-17 12:00:00'</code>. Every table is read at one version for the whole query.</p>
          <p>Tables are created and loaded on the Tables tab (no DDL or DML in SQL).</p>
        </div>
      </section>
    </div>
  );
}

function open(): void {
  const sql = S.draft.sql.trim();
  if (!sql) return toast("Write a query first.");
  const ex = (S.info?.examples ?? []).find((e: Json) => e.id === S.draft.example && e.sql.trim() === sql);
  newQuery(sql, ex ? ex.title.split(" · ")[0] : undefined);
}

/** The options a query compiles with: its engine, and that engine's settings. */
function compileOptions(q: Query) {
  const o = q.options;
  const settings = o.engine === "spark"
    ? { broadcastThreshold: o.spark.broadcastThreshold, shufflePartitions: o.spark.shufflePartitions, maxTaskFailures: o.spark.maxTaskFailures }
    : { shufflePartitions: o.planner.shufflePartitions, joinStrategy: o.planner.joinStrategy, runtimeFilters: o.planner.runtimeFilters };
  return { engine: o.engine, rules: o.rules, cluster: o.cluster, settings };
}

/** The tabs every engine has; the rest are an engine's own. */
const SHARED_TABS: readonly SubTab[] = ["plan", "events", "trace", "result"];

export async function compile(q: Query): Promise<void> {
  if (S.busy) return toast("Another operation is still running.");
  S.busy = true;
  noteAction("compile");
  q.pending = "Compiling";
  update();
  try {
    const r = await call("compile", q.key, q.sql, compileOptions(q));
    q.plans = r.plans;
    q.state = r.state;
    q.compiledSql = q.sql;
    q.events = r.events;
    q.at = null;
    q.hist = new Map([[r.at, r.state]]);
    q.error = null;
    q.planStep = null;
    if (q.sel?.kind !== "node") q.sel = null;
    // another engine has other tabs: one of the old engine's is not there any more
    if (!SHARED_TABS.includes(q.sub) && !viewsOf(r.state.engine).tabs.some((t) => t.key === q.sub)) q.sub = "plan";
  } catch (e) {
    q.error = (e as Error).message;
    q.plans = null;
    q.state = null;
    q.compiledSql = null;
  } finally {
    S.busy = false;
    q.pending = null;
    update();
  }
}

type Action = "step" | "next" | "wave" | "finish" | "run";
const PENDING: Record<Action, (unit: string) => string> = { step: () => "Running a step", next: (u) => `Running to the next ${u}`, wave: () => "Running a wave", finish: () => "Finishing the work running now", run: () => "Running to the end" };

/** The last step; `q.at` is null when the views follow it. */
const lastEvent = (q: Query) => q.events.length - 1;
const shownEvent = (q: Query) => q.at ?? lastEvent(q);

async function act(q: Query, name: Action): Promise<void> {
  if (S.busy) return;
  S.busy = true;
  noteAction(name);
  q.progress = name === "finish" || name === "run" ? { phase: "running" } : null;
  q.pending = PENDING[name](viewsOf(q.state?.engine ?? q.options.engine).unitNoun);
  update();
  try {
    const known = q.events.length;
    // step and next go on from the step shown; the others run on from the latest
    const args = name === "step" || name === "next" ? [shownEvent(q), known] : [known];
    const r = await callWith((e) => { q.progress = e; update(); }, name, q.key, ...args);
    q.events.push(...r.events);
    q.hist.set(r.at, r.state);
    if (r.at === r.total - 1) {
      q.state = r.state;
      q.at = null;
    } else q.at = r.at;
    if (r.state.done && name === "run") q.sub = "result";
  } catch (e) {
    toast((e as Error).message);
  } finally {
    S.busy = false;
    q.pending = null;
    q.progress = null;
    update();
  }
}

async function showStep(q: Query, k: number): Promise<void> {
  k = Math.max(0, Math.min(k, lastEvent(q)));
  if (k >= lastEvent(q)) q.at = null;
  else {
    q.at = k;
    if (!q.hist.has(k)) q.hist.set(k, await call("state", q.key, k));
  }
  update();
}

function Controls({ q }: { q: Query }) {
  const info = S.info;
  const o = q.options;
  const st = q.state;
  const stale = q.compiledSql !== null && q.compiledSql !== q.sql;
  const canRun = !!st && !st.done && !st.error && !stale && !S.busy;
  // Step and Next split also move through steps that already happened.
  const canStep = !!st && !stale && !S.busy && (q.at !== null || canRun);
  const set = (fn: () => void) => { fn(); update(); };
  const isSpark = o.engine === "spark";
  // The views of the engine compiled (or, before that, of the engine chosen).
  const v = viewsOf(st?.engine ?? o.engine);
  const unit = v.unitNoun;
  const auto = isSpark ? 2 * o.cluster.nodes * o.cluster.cores : 2 * o.cluster.nodes;
  const parts = (isSpark ? o.spark.shufflePartitions : o.planner.shufflePartitions) === "auto" ? auto : (isSpark ? o.spark.shufflePartitions : o.planner.shufflePartitions);
  const kb = (b: number) => (b < 0 ? "off" : b >= 1 << 30 ? `${b / (1 << 30)} GB` : b >= 1 << 20 ? `${b / (1 << 20)} MB` : `${b / 1024} KB`);
  const machine = isSpark ? "node" : "worker";
  const engineSummary = isSpark
    ? `Spark-style · broadcast ≤ ${kb(o.spark.broadcastThreshold)}, ${parts} shuffle partitions`
    : `kostka · ${o.planner.joinStrategy} joins, ${parts} partitions${o.planner.runtimeFilters ? ", runtime filters" : ""}`;
  const size = (mb: number) => (mb < 1 ? `${mb * 1024} KB` : mb >= 1024 ? `${mb / 1024} GB` : `${mb} MB`);
  const sel = (label: string, value: string | number, options: [string | number, string][], onChange: (v: string) => void) => (
    <label class="field">{label}
      <select class="in" value={String(value)} onChange={(e) => onChange((e.target as HTMLSelectElement).value)}>{options.map(([v, l]) => <option value={String(v)}>{l}</option>)}</select>
    </label>
  );
  return (
    <div class="stack">
      <details class="compile" data-tour="options">
        <summary>Options · {engineSummary} · {o.rules.length} of {info.rules.length} rules · {o.cluster.nodes} {machine}{o.cluster.nodes === 1 ? "" : "s"} × {o.cluster.cores} core{o.cluster.cores === 1 ? "" : "s"}, {size(o.cluster.memoryMB)} memory, {size(o.cluster.diskMB)} disk, {o.cluster.io} IO · {n(o.cluster.batchRows)}-row batches · {o.cluster.cpu === "measured" ? "measured" : "modelled"} CPU</summary>
        <div class="ogroups">
          <section class="ogroup" data-tour="engine">
            <h4>Engine <span class="faint">who plans and runs the optimized plan</span></h4>
            <div class="row">
              {info.engines.map((e: Json) => (
                <label class={`rule eng-pick ${o.engine === e.name ? "on" : ""}`} data-tour={`engine-${e.name}`}><input type="radio" name={`engine-${q.key}`} checked={o.engine === e.name} onChange={() => set(() => (o.engine = e.name))} /><span><b>{e.label}</b></span></label>
              ))}
              <span class="note grow">{isSpark ? "Spark's model: the plan is cut into stages at every shuffle; each stage runs as tasks, one per partition, in the task slots of executors (one per core). A stage starts when its parents are done." : "kostka's model: the plan is cut into pipelines joined by output buffers; every worker can run a split of any pipeline, one at a time."} The choice applies when you compile.</span>
            </div>
          </section>
          <section class="ogroup" data-tour="rules">
            <h4>1 · Optimizer rules <span class="faint">the logical plan</span></h4>
            <div class="row">
              <span class="note grow">Rewrite the analyzed plan, in this order, each to a fixpoint. They know nothing of the cluster. With none, the analyzed plan runs as written.</span>
              <button class="btn small" onClick={() => set(() => (o.rules = info.rules.map((r: Json) => r.name)))}>Select all</button>
              <button class="btn small" onClick={() => set(() => (o.rules = []))}>Deselect all</button>
            </div>
            <div class="rules">
              {info.rules.map((r: Json) => (
                <label class="rule" data-tour={`rule-${r.name}`}><input type="checkbox" checked={o.rules.includes(r.name)} onChange={(e) => set(() => {
                  o.rules = (e.target as HTMLInputElement).checked ? info.rules.map((x: Json) => x.name).filter((x: string) => x === r.name || o.rules.includes(x)) : o.rules.filter((x) => x !== r.name);
                })} /><span><b>{r.name}</b> <code class="faint">{r.cls}</code><span class="doc">{r.doc}</span></span></label>
              ))}
            </div>
          </section>
          {isSpark ? (
          <section class="ogroup" data-tour="spark-settings">
            <h4>2 · Spark-style planner <span class="faint">the plan → stages and tasks</span></h4>
            <p class="note">Spark's settings. The plan, and so its stages, depends on them and on the cluster.</p>
            <div class="rules">
              <div class="rule" data-tour="broadcast-threshold"><span><b>broadcast threshold</b><span class="doc">A join whose build side is estimated at most this big (rows × row width) is a BroadcastHashJoin: every executor gets the whole build side. Bigger ones are SortMergeJoins: both sides are shuffled by the join keys and sorted. Like Spark's autoBroadcastJoinThreshold (10 MB in Spark, scaled down to these small nodes).</span>
                {sel("", o.spark.broadcastThreshold, [[-1, "off (never broadcast)"], [8 * 1024, "8 KB"], [32 * 1024, "32 KB (default)"], [128 * 1024, "128 KB"], [1 << 20, "1 MB"], [1 << 30, "1 GB (always)"]], (x) => set(() => (o.spark.broadcastThreshold = Number(x))))}</span></div>
              <div class="rule"><span><b>shuffle partitions</b><span class="doc">The tasks of a stage that reads a shuffle (spark.sql.shuffle.partitions). Auto is 2 per core in the cluster.</span>
                {sel("", o.spark.shufflePartitions, [["auto", `auto (nodes × cores × 2 = ${2 * o.cluster.nodes * o.cluster.cores})`], ...[1, 2, 4, 8, 16, 32].map((m) => [m, String(m)] as [number, string])], (x) => set(() => (o.spark.shufflePartitions = x === "auto" ? "auto" : Number(x))))}</span></div>
              <div class="rule"><span><b>max task failures</b><span class="doc">How many attempts a task gets before the query fails (spark.task.maxFailures). A task lost with its node does not count.</span>
                {sel("", o.spark.maxTaskFailures, [1, 2, 4, 8].map((m) => [m, String(m)] as [number, string]), (x) => set(() => (o.spark.maxTaskFailures = Number(x))))}</span></div>
            </div>
          </section>
          ) : (
          <section class="ogroup" data-tour="planner">
            <h4>2 · Physical planner <span class="faint">the plan → pipelines</span></h4>
            <p class="note">Cuts the optimized plan into pipelines and decides how rows move between workers. These choices depend on the cluster.</p>
            <div class="rules">
              <div class="rule" data-tour="joins"><span><b>join distribution</b><span class="doc">Broadcast copies the build side to every worker; shuffle partitions both sides by the join keys. Auto broadcasts when (workers − 1) × build rows ≤ probe rows.</span>
                {sel("", o.planner.joinStrategy, [["auto", "auto (by estimate)"], ["broadcast", "broadcast"], ["shuffle", "shuffle"]], (v) => set(() => (o.planner.joinStrategy = v as never)))}</span></div>
              <div class="rule"><span><b>shuffle partitions</b><span class="doc">Partitions of every shuffle (aggregates and shuffle joins). Each has one owner worker; auto gives each worker two.</span>
                {sel("", o.planner.shufflePartitions, [["auto", `auto (2 × workers = ${2 * o.cluster.nodes})`], ...[1, 2, 4, 8, 16, 32].map((m) => [m, String(m)] as [number, string])], (v) => set(() => (o.planner.shufflePartitions = v === "auto" ? "auto" : Number(v))))}</span></div>
              <label class="rule" data-tour="runtime-filters"><input type="checkbox" checked={o.planner.runtimeFilters} onChange={(e) => set(() => (o.planner.runtimeFilters = (e.target as HTMLInputElement).checked))} />
                <span><b>runtime filters</b><span class="doc">When a hash join's build pipeline is done, its keys skip probe-side files whose statistics miss them, and drop probe rows that cannot match right after the scan.</span></span></label>
            </div>
          </section>
          )}
          <section class="ogroup" data-tour="cluster">
            <h4>3 · Cluster <span class="faint">the machines</span></h4>
            <p class="note">{isSpark
              ? "One executor per node, with a task slot per core. Each running task gets memory / cores: a Sort that outgrows it spills to disk, a hash aggregate fails the task. Shuffle files go to the node's disk; a full disk fails the task."
              : "Output waiting for the next pipeline spills to disk when memory runs short; a full disk fails the query, and so does a hash table or aggregate that doesn't fit in memory. kostka runs one split at a time per worker, on core 0."}</p>
            <div class="row">
              {sel(isSpark ? "Nodes" : "Workers", o.cluster.nodes, info.cluster.nodes.map((w: number) => [w, String(w)]), (v) => set(() => (o.cluster.nodes = Number(v))))}
              <span data-tour="cores">{sel("Cores per node", o.cluster.cores, [1, 2, 4, 8].map((c) => [c, String(c)] as [number, string]), (v) => set(() => (o.cluster.cores = Number(v))))}</span>
              {sel(`Memory per ${machine}`, o.cluster.memoryMB, info.cluster.memory.map((m: number) => [m, size(m)]), (v) => set(() => (o.cluster.memoryMB = Number(v))))}
              {sel(`Disk per ${machine}`, o.cluster.diskMB, info.cluster.disk.map((m: number) => [m, size(m)]), (v) => set(() => (o.cluster.diskMB = Number(v))))}
              {sel("IO speeds", o.cluster.io, info.cluster.io.map((p: Json) => [p.name, p.label]), (v) => set(() => (o.cluster.io = v)))}
            </div>
          </section>
          <section class="ogroup" data-tour="flow">
            <h4>4 · Data flow <span class="faint">how work is cut up</span></h4>
            <p class="note">{isSpark ? "A data file is one task of a scan stage; operators pass rows on in batches. (Split size is kostka's.)" : "A data file is one split; a buffer split is up to the split size of outputs, cut by the coordinator (an aggregate's merge reads whole partitions). Operators pass rows on in batches."}</p>
            <div class="row">
              {sel("Batch size", o.cluster.batchRows, info.cluster.batches.map((m: number) => [m, `${n(m)} rows`]), (v) => set(() => (o.cluster.batchRows = Number(v))))}
              {sel("Split size", o.cluster.splitRows, [256, 1024, 4096, 16384, 65536, 1048576].map((m) => [m, `${n(m)} rows`]), (v) => set(() => (o.cluster.splitRows = Number(v))))}
            </div>
          </section>
          <section class="ogroup" data-tour="sim">
            <h4>5 · Simulation <span class="faint">how time is counted</span></h4>
            <div class="row">
              {sel("CPU time", o.cluster.cpu, info.cluster.cpu.map((m: Json) => [m.name, m.label]), (v) => set(() => (o.cluster.cpu = v as never)))}
              <span class="note grow">{info.cluster.cpu.find((m: Json) => m.name === o.cluster.cpu)?.doc} IO time is always computed from bytes and the IO speeds.</span>
            </div>
          </section>
        </div>
      </details>
      <div class="controls">
        <button class="btn primary" data-tour="compile" disabled={S.busy} onClick={() => compile(q)}>{q.plans ? "Recompile" : "Compile"}</button>
        <button class="btn" data-tour="step" disabled={!canStep} title="Goes to the next step, running the simulation if it has not happened yet" onClick={() => act(q, "step")}>Step</button>
        <button class="btn" data-tour="next" disabled={!canStep} title={`Goes to the next step where a ${v.nodeNoun} ran a ${unit}`} onClick={() => act(q, "next")}>Next {unit}</button>
        <button class="btn" data-tour="wave" disabled={!canRun} title={`Runs until each ${v.nodeNoun} (each core, for tasks) that can ran one ${unit}`} onClick={() => act(q, "wave")}>Wave</button>
        <button class="btn" data-tour="finish" disabled={!canRun} title="Runs until the work running now is done" onClick={() => act(q, "finish")}>Finish {st ? v.running(st) : ""}</button>
        <button class="btn" data-tour="run" disabled={!canRun} onClick={() => act(q, "run")}>Run to the end</button>
        <span class="grow" />
        {q.pending && <Spinner>{q.pending}…</Spinner>}
        {stale && <span class="chip warn">edited: recompile to apply</span>}
        {st && <span class="note">{st.done ? <span class="chip ok">done</span> : st.error ? <span class="chip bad">failed</span> : <span class="chip run">step {n(lastEvent(q))}</span>} {ms(st.elapsed_ms)} simulated</span>}
      </div>
      {q.progress && <div class="stack" style="gap:4px"><ProgressBar done={q.progress.done ?? 0} total={q.progress.total ?? 1} /><span class="note">{q.progress.phase} · {n(q.progress.done ?? 0)} of {n(q.progress.total ?? 0)} {unit}s done</span></div>}
      {q.events.length > 1 && (
        <div class="hist" data-tour="history">
          <div class="row">
            <span class="note">History</span>
            <button class="btn small" disabled={shownEvent(q) === 0} onClick={() => showStep(q, shownEvent(q) - 1)}>◀</button>
            <input type="range" min="0" max={lastEvent(q)} value={shownEvent(q)} onInput={(e) => showStep(q, Number((e.target as HTMLInputElement).value))} />
            <button class="btn small" disabled={q.at === null} onClick={() => showStep(q, shownEvent(q) + 1)}>▶</button>
          </div>
          <span class="note">{q.at === null ? `latest: step ${n(lastEvent(q))}` : `step ${n(q.at)} of ${n(lastEvent(q))}: the views show the query as it was then`}</span>
        </div>
      )}
      {v.controlsNote}
    </div>
  );
}

export function QueryTab({ q }: { q: Query }) {
  const state = shownState(q);
  const views = state ? viewsOf(state.engine) : null;
  // the shared tabs, with the engine's own after Plan
  const tabs: [SubTab, string][] = [["plan", "Plan"], ...(views?.tabs ?? []).map((t): [SubTab, string] => [t.key, t.label]), ["events", "Steps"], ["trace", "Trace"], ["result", "Result"]];
  const own = views?.tabs.find((t) => t.key === q.sub);
  const engine = state?.engine ?? q.options.engine;
  const label = S.info?.engines.find((e: Json) => e.name === engine)?.label ?? engine;
  return (
    <div class={`stack eng-${engine}`}>
      <section class="panel stack" data-tour="query">
        <div class="row"><h2 class="grow">{q.name}</h2><span class="chip eng-chip" data-tour="engine-label" title="The engine this query runs on: choose it in Options">{engine === "spark" ? "Spark-style engine" : label}</span></div>
        <Editor doc={q.sql} catalog={S.catalog} check={check} onChange={(t) => { const was = q.compiledSql !== null && q.compiledSql !== q.sql; q.sql = t; if (was !== (q.compiledSql !== null && q.compiledSql !== q.sql)) update(); }} onRun={() => compile(q)} />
        {q.error && <div class="error">{q.error}</div>}
        {S.info && <Controls q={q} />}
      </section>
      {q.plans && state && (
        <section class="panel stack">
          <div class="subtabs">{tabs.map(([k, l]) => <button data-tour={`sub-${k}`} class={q.sub === k ? "on" : ""} onClick={() => { q.sub = k; update(); }}>{l}{k === "result" && state.done ? ` (${n(state.result.total)})` : ""}</button>)}</div>
          {(q.sub !== "plan" || q.planStep === null) && <EventBanner q={q} state={state} />}
          {state.error && <div class="failbox" data-tour="failbox"><b>The query failed</b>{state.failed && <span class="note">in {state.failed.unit}</span>}<span class="mono">{state.error}</span></div>}
          {q.sub !== "workers" && q.sub !== "trace" && q.sub !== "events" && state.units.length > 0 && <Timeline q={q} state={state} compact />}
          {q.sub === "plan" && <PlanView q={q} state={state} />}
          {own && own.view({ q, state })}
          {q.sub === "events" && <EventsView q={q} state={state} go={(k) => showStep(q, k)} />}
          {q.sub === "trace" && <TraceView q={q} state={state} />}
          {q.sub === "result" && (
            <div class="stack" data-tour="result">
              <p class="note">{state.done ? `${n(state.result.total)} rows${state.result.total > state.result.rows.length ? `, the first ${n(state.result.rows.length)} shown` : ""}, in ${ms(state.elapsed_ms)} of simulated time.` : `Not done yet: ${n(state.result.total)} rows so far.`} Without an ORDER BY the order of the rows depends on the schedule.</p>
              <DataTable columns={q.plans.columns} rows={state.result.rows} />
            </div>
          )}
        </section>
      )}
    </div>
  );
}
