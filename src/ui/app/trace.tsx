// The trace: a collapsible Gantt tree, down to nodes, on one time axis, from the start
// of parsing to the end of the run. Compilation comes first (parse, analyze, each
// optimizer pass and the engine's planning, measured in real time in this browser),
// then execution (simulated), starting where compilation ended: per group of units
// (kostka: per pipeline), then per node, each unit a bar drawn as its engine says.

import { useState } from "preact/hooks";
import type { Json, Query } from "./store.ts";
import { ms } from "./format.ts";
import { viewsOf } from "./engines/index.ts";
import { TimeBar } from "./widgets.tsx";

interface Row { key: string; depth: number; label: string; slices: { start: number; end: number; cls: string; title: string }[]; value: string; kids?: Row[] }

const PHASES: Record<string, string> = { parse: "Parse", analyze: "Analyze", optimize: "Optimizer rules", physical: "Physical planning" };

function Rows({ rows, end, open, toggle, mark }: { rows: Row[]; end: number; open: Set<string>; toggle: (k: string) => void; mark: number }) {
  return (
    <>
      {rows.map((r) => [
        <div class="trow" style={`--depth:${r.depth}`}>
          <div class="tl">
            {r.kids?.length ? <button class="tg" onClick={() => toggle(r.key)}>{open.has(r.key) ? "▾" : "▸"}</button> : <span class="tg" />}
            <span>{r.label}</span>
          </div>
          <div class="tt"><b class="tmark" style={`left:${(100 * mark) / end}%`} />{r.slices.map((s) => <i class={s.cls} style={`left:${(100 * s.start) / end}%;width:max(2px, ${(100 * (s.end - s.start)) / end}%)`} title={s.title} />)}</div>
          <div class="tv">{r.value}</div>
        </div>,
        r.kids && open.has(r.key) && <Rows rows={r.kids} end={end} open={open} toggle={toggle} mark={mark} />,
      ])}
    </>
  );
}

export function TraceView({ q, state }: { q: Query; state: Json }) {
  const [open, setOpen] = useState(new Set<string>(["compile", "exec"]));
  const toggle = (k: string) => {
    const s = new Set(open);
    if (s.has(k)) s.delete(k);
    else s.add(k);
    setOpen(s);
  };
  const timings: Json[] = q.plans.timings;
  const phases: Json[] = q.plans.phases ?? [];
  // Execution starts where compilation ended.
  const at = Math.max(1e-6, ...phases.map((p) => p.end), ...timings.map((t) => t.end));
  const pass = (t: Json, depth: string) => ({ start: t.start, end: t.end, cls: `c ${t.changed ? "chg" : ""}`, title: `${depth}${t.rule} pass ${t.iteration}${t.changed ? " (changed the plan)" : ""}: ${ms(t.end - t.start)}` });
  const byRule = new Map<string, Json[]>();
  for (const t of timings) byRule.set(t.rule, [...(byRule.get(t.rule) ?? []), t]);
  const compile: Row = {
    key: "compile", depth: 0, label: "Compilation (real time)", value: ms(at),
    slices: phases.map((p) => ({ start: p.start, end: p.end, cls: `c ph-${p.phase}`, title: `${PHASES[p.phase]}: ${ms(p.end - p.start)}` })),
    kids: phases.map((p) => p.phase === "optimize"
      ? {
        key: "optimize", depth: 1, label: PHASES.optimize!, value: ms(p.end - p.start), slices: timings.map((t) => pass(t, "")),
        kids: [...byRule].map(([rule, ts]) => ({ key: `r:${rule}`, depth: 2, label: rule, value: `${ts.length} pass${ts.length === 1 ? "" : "es"}`, slices: ts.map((t) => pass(t, "")) })),
      }
      : { key: p.phase, depth: 1, label: PHASES[p.phase] ?? p.phase, value: ms(p.end - p.start), slices: [{ start: p.start, end: p.end, cls: `c ph-${p.phase}`, title: `${PHASES[p.phase]}: ${ms(p.end - p.start)}` }] }),
  };
  const end = at + Math.max(1e-6, state.elapsed_ms ?? 0);
  const v = viewsOf(state.engine);
  const unitSlice = (u: Json) => {
    const look = v.unit(u, q);
    return { start: at + u.start_ms, end: at + u.end_ms, cls: `s ${look.cls}`, title: look.title };
  };
  const units = (k: number) => `${k} ${v.unitNoun}${k === 1 ? "" : "s"}`;
  const exec: Row = {
    key: "exec", depth: 0, label: "Execution (simulated)", value: ms(state.elapsed_ms),
    slices: state.units.map(unitSlice),
    kids: v.groups(state).map((g) => {
      const us = state.units.filter((u: Json) => u.group === g.group);
      return {
        key: `g:${g.group}`, depth: 1, label: g.label, value: g.value,
        slices: us.map(unitSlice),
        kids: [...new Set<number>(us.map((u: Json) => u.node))].sort((a, b) => a - b).map((nd) => {
          const nu = us.filter((u: Json) => u.node === nd);
          const failed = nu.filter((u: Json) => u.failed).length;
          return { key: `n:${g.group}:${nd}`, depth: 2, label: `on ${v.nodeName(nd)}`, value: `${units(nu.length)}${failed ? `, ${failed} failed` : ""}`, slices: nu.map(unitSlice) };
        }),
      };
    }),
  };
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => f * end);
  const total = Object.fromEntries(Object.keys(state.nodes[0]?.time_ms ?? {}).map((c) => [c, state.nodes.reduce((s: number, w: Json) => s + w.time_ms[c], 0)]));
  return (
    <div class="stack" data-tour="trace">
      <section class="panel stack"><h4>Time by category, all {v.nodeNoun}s</h4><TimeBar tm={total} /></section>
      <section class="panel trace">
        <div class="trow taxis"><div class="tl">from the start of parsing</div>
          <div class="tt"><b class="tmark" style={`left:${(100 * at) / end}%`} />{ticks.map((x) => <span style={`left:${(100 * x) / end}%`}>{ms(x)}</span>)}</div>
          <div class="tv">{ms(end)}</div></div>
        <Rows rows={[compile, exec]} end={end} open={open} toggle={toggle} mark={at} />
      </section>
      <p class="note">One time axis: compilation (measured in this browser) and then execution (simulated), which starts where compilation ended (the vertical line, at {ms(at)}). Bars are {v.unitNoun}s, {v.unitColors}. Optimizer passes that changed the plan are darker.</p>
    </div>
  );
}
