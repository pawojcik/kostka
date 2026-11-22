// Tutorials: a few guided scenarios over an example query. A scenario loads the data it
// needs, opens its query, and then shows one step at a time in a callout: what to look
// at, and what to do next. A ring marks the element the step is about (by its
// data-tour attribute). A step with a `done` check moves on by itself once the reader
// has done it; the others have a Next button. Nothing here changes how the workbench
// works: the reader clicks the real controls.

import type { ComponentChildren } from "preact";
import { useEffect, useRef } from "preact/hooks";
import { call } from "../rpc.ts";
import { S, update, type Json, type Query } from "./store.ts";
import { newQuery } from "./query.tsx";
import { refreshTables } from "./tables.tsx";
import { ms } from "./format.ts";

interface Step {
  title: string;
  text: (t: Run) => ComponentChildren;
  /** The data-tour value of the element the step is about. */
  target?: string | ((t: Run) => string);
  /** The one thing the reader should do; the step moves on when `done` is true. */
  todo?: string | ((t: Run) => string);
  done?: (t: Run) => boolean;
  /** Runs when the step is shown, e.g. to remember a number for later steps. */
  enter?: (t: Run) => void;
}

interface Scenario {
  id: string;
  title: string;
  summary: string;
  dataset: "taxi" | "ssb";
  example: string;
  name: string;
  workers: number;
  steps: Step[];
}

/** A scenario in progress. */
interface Run {
  scenario: Scenario;
  step: number;
  q: Query | null;
  preparing: string | null;
  error: string | null;
  /** Taken when the step was entered: counts, the plans and the step count. */
  mark: { actions: Record<string, number>; plans: Json; events: number };
  memo: Record<string, number>;
}

let run: Run | null = null;
let picking = false;

// --- what steps check ------------------------------------------------------------------

const since = (t: Run, action: string) => (S.actions[action] ?? 0) - (t.mark.actions[action] ?? 0);
const latest = (t: Run): Json => t.q?.state ?? null;
const recompiled = (t: Run) => !!t.q?.plans && t.q.plans !== t.mark.plans && t.q.compiledSql !== null;
const pipeOf = (t: Run, table: string): Json => latest(t)?.pipelines.find((p: Json) => p.label.startsWith(`Scan(${table}@`));
const hasRule = (t: Run, rule: string) => !!t.q?.options.rules.includes(rule);
const stageOf = (rule: string) => (S.info?.rules ?? []).findIndex((r: Json) => r.name === rule) + 1;
const optionsOpen = () => !!(document.querySelector("[data-tour=options]") as HTMLDetailsElement | null)?.open;
const sub = (t: Run, k: string) => t.q?.sub === k;
const gone = (t: Run): number[] => (latest(t)?.members ?? []).map((m: Json, i: number) => (m.status === "gone" ? i : -1)).filter((i: number) => i >= 0);
const upWorkers = (t: Run): number[] => (latest(t)?.workers ?? []).map((w: Json) => w.worker).filter((w: number) => latest(t)?.members?.[w]?.status !== "gone");

const B = ({ children }: { children: ComponentChildren }) => <b>{children}</b>;

// Every step asks for at most one action; a step that asks for one is followed by a
// step that points at what the action did.

/** A step that asks for one action. */
const act = (title: string, target: Step["target"], todo: Step["todo"], done: Step["done"], text: Step["text"] = () => null, enter?: Step["enter"]): Step =>
  ({ title, target, todo, done, text, enter });
/** A step that points at something; Next moves on. */
const see = (title: string, target: Step["target"], text: Step["text"]): Step => ({ title, target, text });

const compileLabel = (t: Run) => (t.q?.plans ? "Recompile" : "Compile");
const compile = (title: string, text: Step["text"] = () => null): Step =>
  act(title, "compile", (t) => `Click ${compileLabel(t)}.`, recompiled, text);
const openTab = (k: string, label: string, text: Step["text"] = () => null): Step =>
  act(`Open ${label}`, `sub-${k}`, `Click the ${label} tab.`, (t) => sub(t, k), text);
const runToEnd = (text: Step["text"] = () => null): Step =>
  act("Run to the end", "run", "Click Run to the end.", (t) => !!latest(t)?.done, text);
const openOptions: Step = act("Open the options", "options", "Click Options to open them.", () => optionsOpen(),
  () => <>The options are in five groups, in the order they apply: optimizer rules, the physical planner, the cluster, data flow, and the simulation.</>);
const toggleRule = (rule: string, on: boolean, text: Step["text"] = () => null): Step =>
  act(`${on ? "Turn on" : "Turn off"} ${rule}`, `rule-${rule}`, `${on ? "Check" : "Uncheck"} ${rule}.`, (t) => hasRule(t, rule) === on, text);

const SCENARIOS: Scenario[] = [
  {
    id: "first",
    title: "Your first query",
    summary: "Compile a query, read its plan and program, run it step by step, and look at the result and the time it took.",
    dataset: "taxi", example: "taxi3", name: "Busiest zones", workers: 2,
    steps: [
      see("The query", "query", () => <>This query counts NYC taxi trips per pickup zone and keeps the ten busiest. <B>trips</B> has 100,000 rows and <B>zones</B> 265. kostka will compile it into a plan, then run it on a simulated cluster of 2 workers, one step at a time.</>),
      compile("Compile it", () => <>Compiling parses the SQL, runs the optimizer rules, cuts the plan into pipelines, and starts the simulated cluster: a coordinator and its workers.</>),
      see("The plan", "plan", () => <>This is what Compile made. Read the plan from the bottom up: rows flow from the two scans, through the join (zone ids matched to zone names), into the aggregate (a count per zone), and up to the top 10. The ≈ numbers are the optimizer's estimates, made from table statistics before anything runs.</>),
      act("The plan as written", "stages", "Click stage 0, analyzed.", (t) => t.q?.planStep === 0,
        () => <>The stages above the tree show the plan as the analyzer built it from the SQL, and then after each optimizer rule.</>),
      see("Before the optimizer", "plan", () => <>The plan as written: both scans read every column of their tables.</>),
      act("What a rule did", "stages", "Click the projection_pushdown stage.", (t) => t.q?.planStep === stageOf("projection_pushdown")),
      see("Fewer columns", "plan", () => <>Dashed nodes differ from the stage before. <B>projection_pushdown</B> made each scan read only the columns the query uses: trips has 11, and this query needs one.</>),
      act("Back to the optimized plan", "stages", "Click the last stage.", (t) => t.q?.planStep === null,
        () => <>The last stage is the optimized plan: the one the program is built from.</>),
      openTab("program", "Program", () => <>The plan says what to compute. The program says how.</>),
      see("Pipelines and buffers", "program", () => <>Each card is a <B>pipeline</B>: operators that run together, one <B>split</B> (a piece of input) at a time. P0 scans zones into a broadcast buffer: every worker reads all of it to build a hash table. P1 scans trips, looks each trip's zone up in that table and counts per zone; its output is shuffled into partitions by zone. P2 adds up each partition's counts and keeps its top 10; P3 merges those into the result. Click a card, an operator or a buffer to highlight its lines and see its state next to it.</>),
      act("Take a step", "step", "Click Step.", (t) => since(t, "step") >= 1,
        () => <>The query runs as a sequence of steps: the coordinator making a pipeline's splits and assigning them, a worker processing a split, a message arriving. Compiling already took the first ones.</>),
      see("What the step did", "banner", () => <>This banner says what the step shown did, with the messages it sent. Step again whenever you like: each step is kept.</>),
      act("A wave at a time", "wave", "Click Wave.", (t) => since(t, "wave") >= 1,
        () => <>A wave runs until every worker that has work has processed one more split.</>),
      see("What the wave did", "timeline", () => <>The timeline has a lane per worker and a rectangle per split it ran, colored by pipeline. P1 can only start once each worker can build the zones hash table.</>),
      openTab("state", "Query state", () => <>The Query state shows the query as the coordinator sees it.</>),
      see("Splits and outputs", "state-pipes", () => <>Each square is a split, colored by the worker that ran it: P1 has one per data file of trips. Below are the output buffers: what each split wrote to each partition, and which worker holds it. Hover over a square to see where it came from.</>),
      runToEnd(() => <>Now let it finish.</>),
      see("The result", "result", (t) => <>The ten busiest pickup zones. The time above, {ms(latest(t)?.elapsed_ms ?? 0)}, is <B>simulated</B>: CPU time as measured in this browser, plus IO time computed from the bytes read and sent and the cluster's IO speeds.</>),
      openTab("program", "Program", () => <>Which operators did the work?</>),
      see("Heavy operators", "heavy", () => <>Operators that took 10% or more of the time carry a weight icon in the graph, and are listed here, heaviest first. The list follows the step shown, so it changes as a query runs. Click one to find it in the graph.</>),
      openTab("trace", "Trace", () => <>Where did the time go?</>),
      see("Where the time went", "trace", () => <>One time axis from the start of parsing: compilation (parse, analyze, each optimizer pass, physical planning), measured in this browser, then the simulated execution, from where compilation ended. Open a pipeline to see its splits on each worker, colored by pipeline.</>),
      act("Go back in time", "history", "Drag the History slider back to an early step.", (t) => t.q?.at !== null && t.q?.at !== undefined,
        () => <>Every step is kept.</>),
      see("The query as it was", "banner", () => <>Every view now shows the query as it was at that step; ▶ moves forward one step at a time.</>),
      see("That's the tour", undefined, () => <>You've compiled a query, read its plan and program, run it, and looked at its result, its heavy operators and its timing. Try the other tutorials, change the options and recompile, or open another example from + New query.</>),
    ],
  },
  {
    id: "split",
    title: "How the work is split",
    summary: "Broadcast and shuffle joins: who builds which hash table, how a fact table's files spread over the workers, and what partitions are.",
    dataset: "ssb", example: "ssb2.1", name: "SSB Q2.1", workers: 2,
    steps: [
      see("A star join", "query", () => <>SSB Q2.1 joins the fact table, <B>lineorder</B> (60,000 order lines), with three dimensions, <B>date</B>, <B>part</B> and <B>supplier</B>, and adds up revenue per year and brand. It runs on 2 workers.</>),
      compile("Compile it"),
      see("The plan", "plan", () => <>lineorder joined with date, part and supplier. The filters on part and supplier sit right above their scans, so only matching rows reach the joins.</>),
      openTab("program", "Program", () => <>Let's see how the joins were turned into pipelines.</>),
      see("Broadcast joins", "program", () => <>P0 and P1 scan supplier and part, keep the rows that match the filters, and write <B>broadcast</B> buffers: every worker reads a whole copy (the dashed lines) and builds its own hash table. P2 scans lineorder and probes both tables. Its output is only a few hundred rows, so the planner broadcasts it too, and P3 scans date and probes it. A join is broadcast when copying its build side to every worker costs less than moving the probe side.</>),
      act("Look at a buffer", "program", "Click a buffer node (B0, B1…).", (t) => t.q?.sel?.kind === "buffer"),
      see("A buffer's state", "program", () => <>Its lines are highlighted: the pipeline that writes it and those that read it. The popover next to it says who writes and reads it, how many rows it holds, in how many outputs, and which worker holds how much of each partition. Escape closes it.</>),
      act("Run up to the fact table", "wave", "Click Wave.", (t) => (pipeOf(t, "lineorder")?.splits.done ?? 0) >= 2,
        (t) => <>P2, the lineorder scan, can't start until both hash tables it probes can be built. Keep clicking Wave until it has processed 2 splits: {pipeOf(t, "lineorder")?.splits.done ?? 0} so far.</>),
      see("The waves so far", "timeline", () => <>P0 and P1 ran first; then each worker took P2 splits as they were handed out.</>),
      openTab("state", "Query state", () => <>Each of lineorder's data files is one of P2's splits.</>),
      see("Handing out splits", "state-cluster", () => <>The coordinator queues at most 2 splits per worker and pipeline (the slots in each worker card), and hands out more as each worker reports a split done. A worker that finishes sooner gets more: no split is tied to a worker in advance.</>),
      openTab("workers", "Workers", () => <>Now look at each worker.</>),
      see("Cached hash tables", "workers", () => <>Each worker card lists the hash tables it has cached. With broadcast, every worker builds the same supplier and part tables from its own copy of the buffers: work repeated on every worker, in exchange for never moving the lineorder rows.</>),
      runToEnd(() => <>Let it finish.</>),
      see("The result", "result", (t) => <>Revenue per year and brand, in {ms(latest(t)?.elapsed_ms ?? 0)} of simulated time.</>),
      openTab("state", "Query state", () => <>Now the aggregate.</>),
      see("Partitions", "state-buffers", () => <>The aggregate is split in two: P3 pre-aggregates on each worker and <B>shuffles</B> its groups into partitions by the group keys (2 per worker by default), and P4 merges each partition on the worker that owns it, so every group is counted once. Hover an output to see the split that wrote it.</>),
      { ...openOptions, enter: (t) => { t.memo.broadcast = latest(t)?.elapsed_ms ?? 0; } },
      act("Shuffle the joins", "joins", "Set join distribution to shuffle.", (t) => t.q?.options.planner.joinStrategy === "shuffle",
        () => <>With <B>shuffle</B>, both sides of a join are partitioned by the join key instead, so each partition's hash table is built once, on one worker, and probe rows travel to it.</>),
      compile("Compile with shuffle", () => <>Options apply when you compile.</>),
      openTab("program", "Program"),
      see("Shuffle joins", "program", () => <>Every join now reads both inputs from shuffle buffers, partition by partition, instead of a whole broadcast copy.</>),
      runToEnd(),
      see("Which was faster?", "result", (t) => {
        const b = t.memo.broadcast ?? 0, s = latest(t)?.elapsed_ms ?? 0;
        return <>With broadcast the query took {ms(b)}; with shuffle, {ms(s)}. {b < s
          ? <>The dimension tables are small, so copying them to every worker is cheap, while shuffling moves lineorder rows over the network.</>
          : <>Here shuffle won, though the times vary from run to run with measured CPU time; set CPU time to Cost model in Options for repeatable numbers.</>} With large build sides, shuffle wins clearly; the planner's auto setting decides from row estimates. The Trace tab shows where the time went.</>;
      }),
    ],
  },
  {
    id: "failure",
    title: "A worker fails",
    summary: "Stop a worker in the middle of a query and watch the coordinator recover from lineage: lost outputs, new owners, splits that run again.",
    dataset: "taxi", example: "taxi5", name: "Borough to borough", workers: 3,
    steps: [
      see("The query", "query", () => <>This query counts taxi trips from borough to borough, on 3 workers. You'll stop a worker in the middle of the run.</>),
      compile("Compile it"),
      see("Ready to run", "banner", () => <>Compiling also started the run: the coordinator made the first pipelines' splits and assigned them. This banner always says what the latest step did.</>),
      act("Get some work done", "wave", "Click Wave.", (t) => (pipeOf(t, "trips")?.splits.done ?? 0) >= 3,
        (t) => <>P2 scans trips, looks up both zones, and pre-aggregates. Each split it finishes leaves outputs, partial counts in shuffle partitions, on the worker that ran it. Keep clicking Wave until P2 has finished 3 splits: {pipeOf(t, "trips")?.splits.done ?? 0} so far.</>),
      see("Work in progress", "timeline", () => <>The trips splits are spread over the three workers.</>),
      openTab("workers", "Workers", () => <>Each card shows what a worker holds in memory and its queued splits.</>),
      act("Stop a worker", "stop-1", "Press Stop on W1.", (t) => gone(t).length > 0 || !!t.q?.events.some((e: Json) => e.kind === "worker_gone"),
        () => <>Stop is a failure: W1 is gone with everything it holds: its queued splits, its outputs and its hash tables. It is not replaced.</>),
      see("W1 is gone", "wcard-1", () => <>The coordinator got a WorkerGone event. W1 runs nothing more.</>),
      openTab("state", "Query state", () => <>What does the coordinator do about it?</>),
      see("Recovery from lineage", "state-buffers", () => <>The coordinator knows which split wrote each output. W1's outputs are marked lost, W1's partitions get new owners, and the splits that made the lost outputs go back to the queue: a small number on a square or queue slot is its attempt. Splits are self-contained, so running one again gives the same output.</>),
      runToEnd(() => <>Let the two remaining workers finish.</>),
      see("The same result", "result", () => <>The same 15 rows as without a failure, just later. In the Workers tab's timeline, striped rectangles are splits that ran again.</>),
      compile("Start over", () => <>Now with no survivors.</>),
      see("A fresh run", "banner", () => <>A new run from the start, with all three workers back.</>),
      act("A little work", "wave", "Click Wave.", (t) => since(t, "wave") >= 1),
      openTab("workers", "Workers"),
      act("Stop every worker", (t) => `stop-${upWorkers(t)[0] ?? 0}`, (t) => `Press Stop on W${upWorkers(t)[0] ?? 0}.`, (t) => !!latest(t)?.error,
        (t) => <>Stop them one at a time: {3 - upWorkers(t).length} of 3 stopped.</>),
      see("The query failed", "failbox", () => <>With no worker left, nobody can run the lost splits again. That's the limit of recovery from lineage: the coordinator can always run a split again, but not without a worker to run it on.</>),
    ],
  },
  {
    id: "optimizer",
    title: "What the optimizer saves you",
    summary: "Turn rules off one at a time on a five-table join: see the coordinator refuse a cross join, what each rule changes, and what it costs.",
    dataset: "ssb", example: "ssb4.1", name: "SSB Q4.1", workers: 2,
    steps: [
      see("Five tables", "query", () => <>SSB Q4.1 joins all five SSB tables. It's written the old way, with the join conditions in WHERE and the tables listed in FROM. The optimizer turns that into something that can run. Let's see how.</>),
      openOptions,
      toggleRule("filter_pushdown", false, () => <>filter_pushdown moves each condition as far down as it can go, into a join or a scan.</>),
      compile("Compile without it"),
      see("Refused", "failbox", () => <>The coordinator refused the query before running a single split. Without filter_pushdown, the join conditions stay in one Filter above the joins, so every join is a <B>cross join</B>: every row paired with every row. The estimate is tens of quadrillions of rows.</>),
      see("See it in the plan", "plan", () => <>Four joins with no condition at the bottom, and one big Filter above them all.</>),
      toggleRule("filter_pushdown", true),
      compile("Compile with it"),
      see("No longer refused", "plan", () => <>It compiles, and the coordinator started it: no cross joins in this plan.</>),
      act("What it changed", "stages", "Click stage 1, filter_pushdown.", (t) => t.q?.planStep === stageOf("filter_pushdown")),
      see("Conditions moved down", "plan", () => <>The conditions on a single table (regions, manufacturers) moved down to that table's scan. The ones that compare two tables became join keys, but a condition can only sit on a join that has both of its tables below it. Every one of them involves lineorder, which comes last in FROM, so here all four land on the top join and the joins below are still cross joins.</>),
      act("The next rule", "stages", "Click the join_reorder stage.", (t) => t.q?.planStep === stageOf("join_reorder")),
      see("A new order", "plan", () => <>join_reorder takes the five tables and the join conditions as one group and picks a new order from row estimates, starting from lineorder. It doesn't change any condition: each one follows the new order down to the lowest join that has both its tables, so every join gets one. It needs filter_pushdown to have run first, to know which tables connect and how many rows each filtered table has.</>),
      toggleRule("join_reorder", false, () => <>Now turn it off. Without join_reorder, the joins follow the FROM clause: date, customer, supplier, part, lineorder.</>),
      compile("Compile without it"),
      see("Refused again", "failbox", () => <>filter_pushdown did its part, but in FROM order the join conditions all wait at the top join, as at the filter_pushdown stage: date is joined to customer first with no condition between them, a cross join again, 7 billion rows. Order matters.</>),
      toggleRule("join_reorder", true),
      compile("Compile with every rule"),
      see("Accepted", "banner", () => <>The coordinator started it and assigned the first splits.</>),
      runToEnd(),
      see("A full run", "result", (t) => <>With every rule on: {ms(latest(t)?.elapsed_ms ?? 0)} of simulated time.</>),
      { ...toggleRule("projection_pushdown", false, () => <>projection_pushdown makes each scan read only the columns the query uses.</>), enter: (t) => { t.memo.full = latest(t)?.elapsed_ms ?? 0; } },
      compile("Compile without it"),
      see("Wider scans", "banner", () => <>Compiled and started. Every scan now reads all of its table's columns: 17 for lineorder instead of 6.</>),
      runToEnd(),
      see("The difference", "result", (t) => <>With every rule: {ms(t.memo.full ?? 0)}. Without projection_pushdown: {ms(latest(t)?.elapsed_ms ?? 0)}. The lineorder scan read all 17 columns instead of 6, and every buffer carried the extra columns. On bigger tables, or with slower IO speeds, the gap grows. Turn the rule back on, and try the others.</>),
    ],
  },
];

// --- running a scenario ----------------------------------------------------------------

const enterStep = (t: Run) => {
  t.mark = { actions: { ...S.actions }, plans: t.q?.plans ?? null, events: t.q?.events.length ?? 0 };
  t.scenario.steps[t.step]?.enter?.(t);
};

function go(t: Run, step: number): void {
  if (step >= t.scenario.steps.length) return exit();
  t.step = Math.max(0, step);
  enterStep(t);
  update();
}

function exit(): void {
  run = null;
  update();
}

/** Whether the tables an example needs are loaded (not replaced by another dataset's). */
function loaded(example: string): boolean {
  const ex = (S.info?.examples ?? []).find((e: Json) => e.id === example);
  return !!ex && ex.needs.every((tc: string) => {
    const [t, c] = tc.split(".");
    return (S.catalog.tables as Record<string, { name: string }[]>)[t!]?.some((x) => x.name === c);
  });
}

async function start(sc: Scenario): Promise<void> {
  picking = false;
  const t: Run = { scenario: sc, step: 0, q: null, preparing: null, error: null, mark: { actions: {}, plans: null, events: 0 }, memo: {} };
  run = t;
  update();
  try {
    if (!loaded(sc.example)) {
      if (S.busy) throw new Error("Another operation is still running; try again when it's done.");
      S.busy = true;
      t.preparing = sc.dataset === "taxi" ? "Loading the NYC taxi tables…" : "Generating the SSB tables (scale 0.01)…";
      update();
      try {
        await (sc.dataset === "taxi" ? call("loadTaxi", null) : call("loadSsb", 0.01));
      } finally {
        S.busy = false;
      }
      await refreshTables();
    }
    const ex = (S.info?.examples ?? []).find((e: Json) => e.id === sc.example);
    const q = newQuery(ex.sql, sc.name);
    q.options.cluster.workers = sc.workers;
    t.q = q;
    t.preparing = null;
    enterStep(t);
  } catch (e) {
    t.preparing = null;
    t.error = (e as Error).message;
  }
  update();
}

// --- the picker, the ring and the callout ----------------------------------------------

export function TutorialButton() {
  return <button class="btn small" disabled={!S.ready} onClick={() => { picking = true; update(); }}>Tutorial</button>;
}

export function Tutorial() {
  if (picking) return <Picker />;
  if (run) return <Callout t={run} />;
  return null;
}

function Picker() {
  const close = () => { picking = false; update(); };
  return (
    <div class="tour-modal" onClick={close}>
      <div class="tour-picker panel stack" role="dialog" aria-label="Choose a tutorial" onClick={(e) => e.stopPropagation()}>
        <div class="row"><h2 class="grow">Tutorials</h2><button class="btn small" onClick={close}>Close</button></div>
        <p class="note">Each one opens an example query and walks you through it, one step at a time. A ring marks what each step is about; you click the real controls, and the step moves on once you've done what it asks. The data is loaded first if it isn't already.</p>
        <div class="tour-cards">
          {SCENARIOS.map((sc, i) => (
            <button class="tour-card" onClick={() => void start(sc)}>
              <span class="faint">{i + 1} · {sc.dataset === "taxi" ? "NYC taxi" : "SSB"} · {sc.workers} workers · {sc.steps.length} steps</span>
              <b>{sc.title}</b>
              <span class="note">{sc.summary}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

/** Moves the callout to the corner where it covers the least of the target. */
function place(card: HTMLElement | null, box: DOMRect): void {
  if (!card) return;
  if (innerWidth <= 640) { delete card.dataset.corner; return; }
  const w = card.offsetWidth, h = card.offsetHeight, m = 16;
  const corners: [string, number, number][] = [["br", innerWidth - m - w, innerHeight - m - h], ["bl", m, innerHeight - m - h], ["tr", innerWidth - m - w, 56], ["tl", m, 56]];
  const cover = (x: number, y: number) => Math.max(0, Math.min(x + w, box.right) - Math.max(x, box.left)) * Math.max(0, Math.min(y + h, box.bottom) - Math.max(y, box.top));
  const now = corners.find(([k]) => card.dataset.corner === k) ?? corners[0]!;
  let best = now;
  for (const c of corners) if (cover(c[1], c[2]) < cover(best[1], best[2]) - 1) best = c;
  if (best[0] !== card.dataset.corner) card.dataset.corner = best[0];
}

function Callout({ t }: { t: Run }) {
  const ring = useRef<HTMLDivElement>(null);
  const card = useRef<HTMLElement>(null);
  const step = t.scenario.steps[t.step]!;
  const total = t.scenario.steps.length;
  const target = typeof step.target === "function" ? step.target(t) : step.target;

  // Each frame: put the ring on the target, scroll to it once, and move on when done.
  useEffect(() => {
    let frame = 0, scrolled = false, alive = true;
    const tick = () => {
      if (!alive) return;
      let el = target && t.q && !t.preparing ? (document.querySelector(`[data-tour="${target}"]`) as HTMLElement | null) : null;
      // A control inside closed Options: point at what opens them.
      const shut = el?.closest("details:not([open])");
      if (shut) el = shut.querySelector("summary");
      if (card.current) card.current.dataset.shut = shut ? "1" : "";
      const r = ring.current;
      if (r) {
        const box = el?.getBoundingClientRect();
        if (box && box.width > 0) {
          r.style.display = "block";
          r.style.left = `${box.left - 6}px`;
          r.style.top = `${box.top - 6}px`;
          r.style.width = `${box.width + 12}px`;
          r.style.height = `${box.height + 12}px`;
          if (!scrolled && (box.bottom > innerHeight - 40 || box.top < 0)) el!.scrollIntoView({ block: box.height > innerHeight * 0.6 ? "start" : "center", behavior: "smooth" });
          scrolled = true;
          place(card.current, box);
        } else r.style.display = "none";
      }
      if (t.q && !t.preparing && !S.busy && step.done?.(t)) {
        alive = false;
        go(t, t.step + 1);
        return;
      }
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => { alive = false; cancelAnimationFrame(frame); };
  }, [t, t.step, target]);

  return (
    <>
      <div ref={ring} class="tour-ring" aria-hidden="true" />
      <aside ref={card} class="tour-callout panel stack" aria-live="polite" aria-label={`Tutorial: ${t.scenario.title}`}>
        <div class="row"><span class="faint grow">{t.scenario.title} · {t.step + 1} of {total}</span><button class="btn small" title="End the tutorial" onClick={exit}>×</button></div>
        <div class="tour-bar"><i style={`width:${((t.step + 1) / total) * 100}%`} /></div>
        {t.error ? <><h3>Couldn't start</h3><p class="note">{t.error}</p></>
          : t.preparing ? <><h3>Getting ready</h3><p class="note">{t.preparing}</p></>
            : <>
              <h3>{step.title}</h3>
              {step.todo && <p class="tour-todo">→ {typeof step.todo === "function" ? step.todo(t) : step.todo}</p>}
              {step.todo && <p class="tour-hint note">It's inside Options, which are closed: click Options (marked) to open them first.</p>}
              <p>{step.text(t)}</p>
            </>}
        {!t.preparing && !t.error && (
          <div class="row">
            <button class="btn small" disabled={t.step === 0} onClick={() => go(t, t.step - 1)}>Back</button>
            <span class="grow" />
            {step.done
              ? <button class="link" onClick={() => go(t, t.step + 1)}>skip this step</button>
              : <button class="btn small primary" onClick={() => go(t, t.step + 1)}>{t.step === total - 1 ? "Finish" : "Next"}</button>}
          </div>
        )}
      </aside>
    </>
  );
}
