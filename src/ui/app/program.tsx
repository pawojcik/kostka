// The program as a graph: pipelines and output buffers are nodes, joined by lines
// along which data flows. A pipeline's sink writes its buffer; a buffer is read by
// the source of the next pipeline, or read whole by an operator (a hash join's build
// side). Columns follow the data: scans first, the result last. On a wide container
// it flows left to right, on a narrow one top to bottom. An operator taking a large
// share of the query's time so far is marked heavy, and the heavy ones are listed
// under the graph. Clicking a pipeline, an operator or a buffer highlights its lines
// and shows what it is doing right after the step shown, in a popover next to it; on
// entering the tab the view scrolls to the pipeline the step is about (or one running).

import { useLayoutEffect, useRef, useState } from "preact/hooks";
import { update, type Json, type Query } from "./store.ts";
import { bytes, ms, n, pc } from "./format.ts";
import { Chip, KV, StateView, TimeBar } from "./widgets.tsx";
import { Bar } from "./state.tsx";

/** An operator is heavy from this share of all operators' time so far. */
export const HEAVY = 0.1;
const opTime = (o: Json) => Object.values(o.metrics.time_ms as Record<string, number>).reduce((a, b) => a + b, 0);
/** The popover's width, as in the CSS. */
const POP_W = 360;

/** A weight: the mark of a heavy operator. */
export function HeavyIcon({ title }: { title?: string }) {
  return (
    <svg class="heavy" viewBox="0 0 16 16" width="13" height="13" aria-label="heavy" role="img">
      {title && <title>{title}</title>}
      <path d="M5.5 5.2a2.5 2.5 0 1 1 5 0" fill="none" stroke="currentColor" stroke-width="1.6" />
      <path d="M4.2 5.6h7.6l2 8.4H2.2z" fill="currentColor" />
    </svg>
  );
}

/** The elements a selection stands for, as graph keys (an operator, a pipeline's
 * operators, a buffer). */
function selKeys(q: Query, state: Json): Set<string> {
  const s = q.sel;
  if (!s) return new Set();
  if (s.kind === "op") return new Set([`op:${s.id}`]);
  if (s.kind === "pipe") return new Set((state.pipelines[Number(s.id)]?.ops ?? []).map((o: Json) => `op:${o.id}`));
  if (s.kind === "buffer") return new Set([`buf:${s.id}`]);
  return new Set();
}

/** The graph element the popover sits next to. */
function selAnchor(q: Query): string | null {
  const s = q.sel;
  if (!s) return null;
  return s.kind === "op" ? `op:${s.id}` : s.kind === "pipe" ? `pipe:${s.id}` : s.kind === "buffer" ? `buf:${s.id}` : null;
}
const STATUS_CHIP: Record<string, string> = { done: "ok", running: "run", waiting: "" };
const BSTATUS: Record<string, string> = { empty: "", filling: "run", sealed: "ok", released: "skip" };
/** Below this width the graph flows top to bottom. */
const WIDE = 900;

export const OP_HELP: Record<string, string> = {
  Scan: "Reads one data file per split: only the scan's columns, after file pruning.",
  Filter: "Keeps the rows whose condition is TRUE.",
  Project: "Computes the output columns.",
  Limit: "Keeps the first n rows of each split; a Limit that is not alone in its pipeline writes a gather buffer, and once that holds n rows the coordinator skips the pending splits. A second pipeline then keeps n in all.",
  RuntimeFilter: "Drops probe rows whose join key is not among a later join's build keys, right after the scan.",
  HashProbe: "Looks each row up in the hash table built from the other side, on the first split that needs it. The table stays in the worker's cache (a broadcast one per worker, a shuffled one per partition) until the build buffer is released. Its output goes on in batches.",
  Broadcast: "Writes every row to one partition that every worker reads whole (the build side of a broadcast join).",
  Shuffle: "Hash-partitions rows by key into the output buffer's partitions; it writes a piece whenever a partition's rows reach the batch size, and at the end of the split. The pieces stay on this worker.",
  PartialAggregate: "Folds a split's rows into groups and writes them at the end of the split, hash-partitioned by key.",
  MergeAggregate: "Merges the partial groups of one partition (a split is always a whole partition).",
  SortSink: "Buffers a split's rows and sorts them at its end: one sorted run per split.",
  TopNSink: "Keeps a split's best n rows in a heap.",
  MergeSort: "Merges the sorted runs of every split, in one split.",
  Result: "Sends the rows to the coordinator, as each split ends.",
};

export function help(label: string): string | undefined {
  const k = Object.keys(OP_HELP).find((x) => label.startsWith(x));
  return k ? OP_HELP[k] : undefined;
}

export function findOp(state: Json, id: string): [Json, Json] | null {
  for (const p of state.pipelines) for (const o of p.ops) if (o.id === id) return [p, o];
  return null;
}

// --- layout -----------------------------------------------------------------------

type NodeRef = { kind: "pipe"; index: number } | { kind: "buf"; index: number };

/** Columns of nodes: a scan pipeline in column 0, a buffer one after its producer, a
 * pipeline one after the last buffer it reads. Within a column, nodes are ordered by
 * where their neighbors are, to keep lines from crossing. */
function layout(state: Json): NodeRef[][] {
  const pcol = new Map<number, number>();
  const bcol = (b: Json): number => colOf(b.producer) + 1;
  const inputs = (p: Json): Json[] => state.buffers.filter((b: Json) => p.ops.some((o: Json) => o.reads.includes(b.index)));
  function colOf(i: number): number {
    if (pcol.has(i)) return pcol.get(i)!;
    const c = Math.max(-1, ...inputs(state.pipelines[i]).map(bcol)) + 1;
    pcol.set(i, c);
    return c;
  }
  const cols: NodeRef[][] = [];
  const put = (c: number, r: NodeRef) => (cols[c] ??= []).push(r);
  for (const p of state.pipelines) put(colOf(p.index), { kind: "pipe", index: p.index });
  for (const b of state.buffers) put(bcol(b), { kind: "buf", index: b.index });
  const next = (r: NodeRef): NodeRef[] => r.kind === "pipe"
    ? state.buffers.filter((b: Json) => b.producer === r.index).map((b: Json) => ({ kind: "buf", index: b.index }))
    : state.pipelines.filter((p: Json) => p.ops.some((o: Json) => o.reads.includes(r.index))).map((p: Json) => ({ kind: "pipe", index: p.index }));
  const key = (r: NodeRef) => `${r.kind}${r.index}`;
  const pos = new Map<string, number>();
  const place = (c: NodeRef[]) => c.forEach((r, i) => pos.set(key(r), c.length > 1 ? i / (c.length - 1) : 0.5));
  const mean = (xs: NodeRef[]) => (xs.length ? xs.reduce((a, x) => a + (pos.get(key(x)) ?? 0.5), 0) / xs.length : 0.5);
  const filled = cols.filter(Boolean);
  const prev = new Map<string, NodeRef[]>();
  for (const c of filled) for (const r of c) for (const x of next(r)) prev.set(key(x), [...(prev.get(key(x)) ?? []), r]);
  // right to left by successors (the result's column is fixed), then left to right by predecessors
  place(filled[filled.length - 1]!);
  for (let c = filled.length - 2; c >= 0; c--) {
    filled[c]!.sort((a, b) => mean(next(a)) - mean(next(b)));
    place(filled[c]!);
  }
  for (let c = 1; c < filled.length; c++) {
    filled[c]!.sort((a, b) => mean(prev.get(key(a)) ?? []) - mean(prev.get(key(b)) ?? []));
    place(filled[c]!);
  }
  return filled;
}

interface Edge { from: string; to: string; whole: boolean; buffer: number }

/** The lines: a sink into its buffer, a buffer into each operator that reads it. */
function edges(state: Json): Edge[] {
  const out: Edge[] = [];
  for (const p of state.pipelines) {
    for (const o of p.ops) {
      if (o.writes !== null && o.writes !== undefined) out.push({ from: `op:${o.id}`, to: `buf:${o.writes}`, whole: false, buffer: o.writes });
      for (const b of o.reads) out.push({ from: `buf:${b}`, to: `op:${o.id}`, whole: o.role !== "source", buffer: b });
    }
  }
  return out;
}

// --- nodes ------------------------------------------------------------------------

function OpCard({ q, o, total }: { q: Query; o: Json; total: number }) {
  const t = opTime(o);
  const share = total ? t / total : 0;
  const sel = q.sel?.kind === "op" && q.sel.id === o.id;
  return (
    <div data-k={`op:${o.id}`} class={`op gnode ${sel ? "sel" : ""}`} title={help(o.label)} onClick={(e) => { e.stopPropagation(); q.sel = sel ? null : { kind: "op", id: o.id }; update(); }}>
      <div class="role row"><span class="grow">{o.role} · {o.id}</span>{share >= HEAVY && <HeavyIcon title={`heavy: ${Math.round(share * 100)}% of the time so far`} />}</div>
      <div class="nm">{o.label}</div>
      <div class="mx1"><span>{n(o.metrics.rows_in)} → {n(o.metrics.rows_out)}</span><span><span class="pct">{share >= 0.005 ? `${Math.round(share * 100)}%` : ""}</span>{ms(t)}</span></div>
    </div>
  );
}

function PipeNode({ q, p, state, total }: { q: Query; p: Json; state: Json; total: number }) {
  const sel = q.sel?.kind === "pipe" && q.sel.id === p.index;
  const hot = state.event?.pipeline === p.index;
  return (
    <div data-k={`pipe:${p.index}`} class={`pcard ${p.status} ${sel ? "sel" : ""} ${hot ? "hot" : ""}`}>
      <button class="phead" onClick={(e) => { e.stopPropagation(); q.sel = sel ? null : { kind: "pipe", id: p.index }; update(); }}>
        <i class={`sw ${pc(p.index)}`} /><b>P{p.index}</b>
        <Chip kind={STATUS_CHIP[p.status]}>{p.status}</Chip>{p.completions > 1 && <Chip kind="warn">{p.completions}×</Chip>}
        <span class="grow" />
        <span class="faint">{n(p.splits.done)}/{n(p.splits.total)}</span>
      </button>
      {p.splits.total > 0 && <Bar p={p} />}
      <div class="pops">
        {p.ops.map((o: Json, i: number) => [i > 0 && <div class="gdown">↓</div>, <OpCard q={q} o={o} total={total} />])}
      </div>
    </div>
  );
}

function BufNode({ q, b, state }: { q: Query; b: Json; state: Json }) {
  const sel = q.sel?.kind === "buffer" && q.sel.id === b.index;
  const held = b.outputs.filter((o: Json) => o.status === "available");
  const lost = b.outputs.filter((o: Json) => o.status === "lost").length;
  const holders = [...new Set<number>(held.map((o: Json) => o.worker))].sort((x, y) => x - y);
  const hot = state.event?.buffer === b.index;
  return (
    <button data-k={`buf:${b.index}`} class={`bnode ${b.status} ${sel ? "sel" : ""} ${hot ? "hot" : ""}`} onClick={(e) => { e.stopPropagation(); q.sel = sel ? null : { kind: "buffer", id: b.index }; update(); }}>
      <div class="row"><b>B{b.index}</b><span class="grow mono faint">{b.mode}{b.partitions > 1 ? ` · ${b.partitions} parts` : ""}</span><Chip kind={BSTATUS[b.status]}>{b.status}</Chip></div>
      <div class="note">{b.label}</div>
      <div class="xstrip">{held.map((o: Json) => <i class={`wk-${o.worker % 8}`} style={`flex-grow:${Math.max(1, o.rows)}`} />)}</div>
      <div class="faint bmeta">
        {b.status === "released" ? "released: every reader is done" : `${n(b.rows)} rows · ${n(held.length)} outputs${holders.length ? ` on ${holders.map((w) => `W${w}`).join(", ")}` : ""}`}
        {lost > 0 && <span class="lostn"> · {n(lost)} lost</span>}
      </div>
    </button>
  );
}

// --- the view ---------------------------------------------------------------------

/** Where the view scrolls to on entering the tab: the pipeline the step is about, or
 * one running, or the first waiting, or the result's. */
function focusPipeline(state: Json): number {
  const e = state.event;
  if (e?.pipeline !== null && e?.pipeline !== undefined) return e.pipeline;
  const running = state.pipelines.find((p: Json) => p.status === "running");
  if (running) return running.index;
  const waiting = state.pipelines.find((p: Json) => p.status === "waiting");
  return waiting ? waiting.index : state.pipelines.length - 1;
}

export function ProgramView({ q, state }: { q: Query; state: Json }) {
  const outer = useRef<HTMLDivElement>(null);
  const inner = useRef<HTMLDivElement>(null);
  const [wide, setWide] = useState(true);
  const [paths, setPaths] = useState<{ d: string; cls: string; title: string }[]>([]);
  const [size, setSize] = useState({ w: 0, h: 0 });
  const [pop, setPop] = useState<{ x: number; y: number } | null>(null);
  const keys = selKeys(q, state);
  const anchor = selAnchor(q);
  const total = state.pipelines.reduce((s: number, p: Json) => s + p.ops.reduce((a: number, o: Json) => a + opTime(o), 0), 0);
  const cols = layout(state);
  const es = edges(state);

  const measure = () => {
    const box = inner.current;
    if (!box) return;
    const base = box.getBoundingClientRect();
    const rect = (k: string) => {
      const el = box.querySelector(`[data-k="${k}"]`);
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return { x: r.left - base.left, y: r.top - base.top, w: r.width, h: r.height };
    };
    const out = es.flatMap((e) => {
      const a = rect(e.from), b = rect(e.to);
      if (!a || !b) return [];
      let d: string;
      if (wide) {
        const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = b.x, y2 = b.y + b.h / 2, dx = Math.max(24, (x2 - x1) / 2);
        d = `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
      } else if (e.to.startsWith("buf:") || !e.whole) {
        // down into a buffer, or into a pipeline's source (the top of its card)
        const x1 = a.x + a.w / 2, y1 = a.y + a.h, x2 = b.x + b.w / 2, y2 = b.y, dy = Math.max(20, (y2 - y1) / 2);
        d = `M${x1},${y1} C${x1},${y1 + dy} ${x2},${y2 - dy} ${x2},${y2}`;
      } else {
        // into an operator inside a card: around the ops above it, in from the left
        const x1 = a.x + a.w / 2, y1 = a.y + a.h, x2 = b.x, y2 = b.y + b.h / 2;
        d = `M${x1},${y1} C${x1},${y1 + 60} ${x2 - 80},${y2} ${x2},${y2}`;
      }
      const buf = state.buffers[e.buffer];
      const on = keys.has(e.from) || keys.has(e.to);
      const cls = `${e.whole ? "whole" : ""} ${buf?.status === "released" ? "rel" : ""} ${buf?.status === "filling" ? "live" : ""} ${keys.size ? (on ? "on" : "dim") : ""}`;
      const title = e.from.startsWith("buf:") ? `B${e.buffer} → ${e.to.slice(3)}${e.whole ? " (read whole: a hash table's build side)" : ""}` : `${e.from.slice(3)} writes B${e.buffer}`;
      return [{ d, cls, title }];
    });
    // the selection's lines on top
    out.sort((x, y) => Number(x.cls.includes(" on")) - Number(y.cls.includes(" on")));
    const sz = { w: box.scrollWidth, h: box.scrollHeight };
    // the popover: right of its element, else left of it, else below it
    const r = anchor ? rect(anchor) : null;
    let at: { x: number; y: number } | null = null;
    if (r) {
      if (r.x + r.w + 14 + POP_W <= Math.max(sz.w, (outer.current?.clientWidth ?? 0) + (outer.current?.scrollLeft ?? 0))) at = { x: r.x + r.w + 14, y: r.y };
      else if (r.x - 14 - POP_W >= 0) at = { x: r.x - 14 - POP_W, y: r.y };
      else at = { x: Math.max(0, r.x), y: r.y + r.h + 10 };
    }
    setPaths((old) => (JSON.stringify(old) === JSON.stringify(out) ? old : out));
    setSize((old) => (old.w === sz.w && old.h === sz.h ? old : sz));
    setPop((old) => (old?.x === at?.x && old?.y === at?.y ? old : at));
  };

  useLayoutEffect(measure);
  useLayoutEffect(() => {
    const el = outer.current;
    if (!el) return;
    setWide(el.clientWidth >= WIDE);
    const ro = new ResizeObserver(() => setWide(el.clientWidth >= WIDE));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  // on entering the tab, and when the orientation changes: the pipeline that matters now
  useLayoutEffect(() => {
    const el = outer.current;
    const target = el?.querySelector(`[data-k="pipe:${focusPipeline(state)}"]`);
    if (!el || !target) return;
    const o = el.getBoundingClientRect(), t = target.getBoundingClientRect();
    el.scrollLeft += t.left - o.left - Math.max(0, (el.clientWidth - t.width) / 2);
    el.scrollTop += t.top - o.top - Math.max(0, (el.clientHeight - t.height) / 2);
  }, [wide]);

  // Escape closes the popover
  useLayoutEffect(() => {
    const key = (e: KeyboardEvent) => { if (e.key === "Escape" && q.sel) { q.sel = null; update(); } };
    addEventListener("keydown", key);
    return () => removeEventListener("keydown", key);
  }, [q]);

  const panel = !!anchor;
  return (
    <div class="stack">
      <p class="note">
        A pipeline is a chain of operators from a source to a sink, and every worker has every pipeline. Its input is cut into <b>splits</b>: a data file, or a set of outputs of an output buffer. A split is self-contained: its operators start fresh and its sink flushes at its end (only hash tables built from a whole buffer are kept, in the worker's cache). A sink writes its pipeline's <b>output buffer</b>, whose pieces stay on the workers that wrote them until the next pipelines have read them. Lines show where data goes; a dashed line is a buffer read whole (a hash join's build side).
      </p>
      <div data-tour="program" class={`pgwrap ${wide ? "wide" : "narrow"}`}>
        <div class="pg-scroll" ref={outer}>
          <div class={`pgraph ${wide ? "lr" : "tb"} ${panel ? "selecting" : ""}`} ref={inner} onClick={() => { if (q.sel) { q.sel = null; update(); } }}>
            <svg class="pedges" width={size.w} height={size.h}>
              <defs><marker id="parrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L8,4 L0,8 z" /></marker></defs>
              {paths.map((p) => <path d={p.d} class={p.cls} marker-end="url(#parrow)"><title>{p.title}</title></path>)}
            </svg>
            {cols.map((c) => (
              <div class="pcol">
                {c.map((r) => (r.kind === "pipe"
                  ? <PipeNode q={q} p={state.pipelines[r.index]} state={state} total={total} />
                  : <BufNode q={q} b={state.buffers[r.index]} state={state} />))}
              </div>
            ))}
            {panel && pop && <div class="pgpop" style={`left:${pop.x}px;top:${pop.y}px`} onClick={(e) => e.stopPropagation()}><SelPanel q={q} state={state} /></div>}
          </div>
        </div>
      </div>
      {!panel && <p class="note">Click a pipeline, an operator or an output buffer to highlight its lines and see its state right after the step shown.</p>}
      <HeavyList q={q} state={state} total={total} />
    </div>
  );
}

/** The heavy operators so far, heaviest first; clicking one selects it. */
function HeavyList({ q, state, total }: { q: Query; state: Json; total: number }) {
  const heavy = state.pipelines.flatMap((p: Json) => p.ops.map((o: Json) => ({ p, o, t: opTime(o) })))
    .filter((x: Json) => total && x.t / total >= HEAVY).sort((a: Json, b: Json) => b.t - a.t);
  return (
    <section class="panel stack heavylist" data-tour="heavy">
      <h4 class="row"><HeavyIcon /> Heavy operators <span class="faint">· {Math.round(HEAVY * 100)}% or more of the operators' time so far</span></h4>
      {!heavy.length
        ? <p class="note">{total ? "None: the time is spread out." : "None yet: no operator has run."}</p>
        : (
          <table class="data">
            <thead><tr><th>operator</th><th>pipeline</th><th class="n">share</th><th class="n">time</th><th class="n">rows in → out</th></tr></thead>
            <tbody>
              {heavy.map(({ p, o, t }: Json) => (
                <tr class={q.sel?.kind === "op" && q.sel.id === o.id ? "on" : ""}>
                  <td><button class="link" onClick={() => { q.sel = { kind: "op", id: o.id }; update(); document.querySelector(`[data-k="op:${o.id}"]`)?.scrollIntoView({ block: "nearest", inline: "nearest" }); }}>{o.id} {o.label}</button></td>
                  <td><i class={`sw ${pc(p.index)}`} /> P{p.index}</td>
                  <td class="n">{Math.round((100 * t) / total)}%</td>
                  <td class="n">{ms(t)}</td>
                  <td class="n">{n(o.metrics.rows_in)} → {n(o.metrics.rows_out)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
    </section>
  );
}

// --- details: the state right after the step shown --------------------------------

export function SelPanel({ q, state }: { q: Query; state: Json }) {
  const s = q.sel;
  if (!s) return null;
  if (s.kind === "op") {
    const f = findOp(state, String(s.id));
    if (!f) return null;
    const [p, o] = f;
    const notes = (state.units ?? []).filter((u: Json) => u.pipeline === p.index).flatMap((u: Json) => u.ops[p.ops.indexOf(o)]?.notes ?? []).slice(-6);
    return (
      <section class="panel stack">
        <div class="row"><h3 class="grow"><i class={`sw ${pc(p.index)}`} /> {o.id} {o.label}</h3><button class="x" onClick={() => { q.sel = null; update(); }}>×</button></div>
        <p class="note">The {o.role} of P{p.index} ({p.status}). {help(o.label)}</p>
        <KV pairs={[
          ["rows in → out", `${n(o.metrics.rows_in)} → ${n(o.metrics.rows_out)}`],
          ["batches in → out", `${n(o.metrics.batches_in)} → ${n(o.metrics.batches_out)}`],
          ["bytes in → out", `${bytes(o.metrics.bytes_in)} → ${bytes(o.metrics.bytes_out)}`],
          ["splits run", n(o.metrics.runs)],
        ]} />
        <TimeBar tm={o.metrics.time_ms} />
        <h4>State</h4><StateView state={o.state} />
        {notes.length > 0 && <><h4>Notes</h4><ul>{notes.map((x: string) => <li class="note">{x}</li>)}</ul></>}
      </section>
    );
  }
  if (s.kind === "pipe") return <PipePanel q={q} state={state} p={state.pipelines[Number(s.id)]} />;
  if (s.kind === "buffer") return <BufferPanel q={q} state={state} b={state.buffers[Number(s.id)]} />;
  return null;
}

function PipePanel({ q, state, p }: { q: Query; state: Json; p: Json }) {
  const runs = state.units.filter((u: Json) => u.pipeline === p.index);
  const by = new Map<number, number>();
  for (const u of runs) by.set(u.worker, (by.get(u.worker) ?? 0) + 1);
  const queued = state.splits.filter((x: Json) => x.pipeline === p.index && x.status === "queued");
  return (
    <section class="panel stack">
      <div class="row"><h3 class="grow"><i class={`sw ${pc(p.index)}`} /> P{p.index} <Chip kind={STATUS_CHIP[p.status]}>{p.status}</Chip></h3><button class="x" onClick={() => { q.sel = null; update(); }}>×</button></div>
      <p class="mono note">{p.label}</p>
      {p.splits.total > 0 && <Bar p={p} />}
      <KV pairs={[
        ["status", p.completions > 1 ? `${p.status} (done ${p.completions} times: it remade lost outputs)` : p.status === "waiting" ? `waiting for ${p.depends_on.map((i: number) => `P${i}`).join(", ")}` : p.status],
        ["reads", p.reads === "files" ? "data files, a split each" : p.whole ? "a whole buffer, in one split" : "a buffer's outputs, by partition, each on its owner"],
        ["splits", `${n(p.splits.total)}: ${n(p.splits.done)} done, ${n(p.splits.queued)} queued, ${n(p.splits.pending)} not given out${p.splits.blocked ? ` (${n(p.splits.blocked)} blocked)` : ""}${p.splits.skipped ? `, ${n(p.splits.skipped)} skipped` : ""}${p.splits.again ? `, ${n(p.splits.again)} ran again` : ""}`],
        ["queued now", queued.length ? queued.map((x: Json) => `${x.id} on W${x.worker}${x.attempt > 1 ? ` (attempt ${x.attempt})` : ""}`).join(", ") : "nothing"],
        ...(p.skipped_files.length ? [["skipped by runtime filters", `${n(p.skipped_files.length)} files`] as [string, string]] : []),
        ["ran", p.start_ms === null ? "not yet" : `${ms(p.start_ms)} → ${p.end_ms === null ? "…" : ms(p.end_ms)}`],
        ["on", [...by].map(([w, k]) => `W${w} ${n(k)}×`).join(", ") || "nowhere yet"],
        ...(Object.keys(p.owners).length ? [["partition owners", Object.entries(p.owners).map(([k, w]) => `#${k}→W${w}`).join(" ")] as [string, string]] : []),
      ]} />
      <button class="link" onClick={() => { q.sub = "state"; update(); }}>See its splits in the query state</button>
    </section>
  );
}

function BufferPanel({ q, state, b }: { q: Query; state: Json; b: Json }) {
  const consumers = b.consumers.map((c: number) => `P${c}`).join(", ") || "the result";
  const count = (st: string) => b.outputs.filter((o: Json) => o.status === st).length;
  const workers = [...new Set<number>(b.outputs.map((o: Json) => o.worker))].sort((x, y) => x - y);
  const rowsOf = (part: number, w: number) => b.outputs.filter((o: Json) => o.partition === part && o.worker === w && o.status === "available").reduce((a: number, o: Json) => a + o.rows, 0);
  const max = Math.max(1, ...Array.from({ length: b.partitions }, (_, i) => workers.reduce((a, w) => a + rowsOf(i, w), 0)));
  const filters = state.filters.filter((f: Json) => f.producer === b.producer);
  return (
    <section class="panel stack">
      <div class="row"><h3 class="grow">B{b.index} · {b.mode} <Chip kind={BSTATUS[b.status]}>{b.status}</Chip></h3><button class="x" onClick={() => { q.sel = null; update(); }}>×</button></div>
      <p class="note">{b.label}: written by P{b.producer}, read by {consumers}. Each split of P{b.producer} keeps its outputs on the worker it ran on.</p>
      <KV pairs={[
        ["partitions", n(b.partitions)],
        ["rows held", n(b.rows)],
        ["outputs", `${n(count("available"))} held${count("lost") ? `, ${n(count("lost"))} lost` : ""}${count("released") ? `, ${n(count("released"))} released` : ""}`],
        ...(b.enough !== null && b.enough !== undefined ? [["enough at", `${n(b.enough)} rows (a Limit)`] as [string, string]] : []),
        ...filters.map((f: Json) => [`runtime filter ${f.id}`, f.keys === null ? "keys not merged yet" : `${n(f.keys)} keys${f.range ? `, ${f.range[0]} to ${f.range[1]}` : ""}`] as [string, string]),
      ]} />
      {b.rows > 0 && (
        <div class="bparts">
          {Array.from({ length: b.partitions }, (_, i) => (
            <div class="bpart">
              <span class="mono faint">#{i}</span>
              <div class="bbar">{workers.map((w) => { const r = rowsOf(i, w); return r ? <i class={`wk-${w % 8}`} style={`width:${(100 * r) / max}%`} title={`W${w} holds ${n(r)} rows of #${i}`} /> : null; })}</div>
              {b.owners[i] !== undefined ? <span class={`wtag wk-${b.owners[i] % 8}`} title="owner">W{b.owners[i]}</span> : <span />}
            </div>
          ))}
        </div>
      )}
      <button class="link" onClick={() => { q.sub = "state"; update(); }}>See its outputs in the query state</button>
    </section>
  );
}
