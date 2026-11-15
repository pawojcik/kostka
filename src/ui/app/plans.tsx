// Plan trees, root on top and data flowing up, nodes joined by lines (drawn in CSS:
// a stem under each parent, a bar over its children). Clicking a node opens a panel next to
// it: its output columns, the optimizer's row estimate, the rows it actually produced
// (once the query ran) and the physical operators compiled from it.

import { update, type Json, type Query } from "./store.ts";
import { ms, n } from "./format.ts";

const NODE_HELP: Record<string, string> = {
  Scan: "Reads one version of a table: one split per data file. Projection pushdown narrows its columns; file pruning its files.",
  Filter: "Keeps the rows for which the condition is TRUE (not FALSE or NULL).",
  Project: "Computes its output columns from its input.",
  Aggregate: "Groups rows by its keys and folds each group with its aggregates. Runs in two phases: partial per split, then merged per partition.",
  Join: "A hash join: the right side is built into a hash table, the left side probes it.",
  Sort: "Sorts all its rows: each split sorts what it got, then one split merges the sorted runs.",
  Limit: "Passes on the first n rows; without an ORDER BY below it, which rows is not defined.",
  TopN: "ORDER BY with LIMIT: keeps only the best n rows of each split, then merges them.",
};

export function opsOfNode(state: Json, node: number): Json[] {
  return (state?.pipelines ?? []).flatMap((p: Json) => p.ops.filter((o: Json) => o.node === node).map((o: Json) => ({ ...o, pipeline: p.index })));
}

/** Rows a node produced: out of the last of its operators that is not a sink. */
function actualRows(state: Json, node: number): number | null {
  const ops = opsOfNode(state, node).filter((o) => o.role !== "sink");
  if (!ops.length || !state?.units_run) return null;
  const last = ops[ops.length - 1];
  return last.metrics.rows_out;
}

function NodePanel({ q, node, state, linkOps }: { q: Query; node: Json; state: Json; linkOps: boolean }) {
  const ops = linkOps ? opsOfNode(state, node.id) : [];
  const actual = linkOps ? actualRows(state, node.id) : null;
  return (
    <div class="npanel stack" onClick={(e) => e.stopPropagation()}>
      <div class="row"><b class="grow">{node.kind}</b><button class="x" onClick={() => { q.sel = null; update(); }}>×</button></div>
      {NODE_HELP[node.kind] && <p class="note">{NODE_HELP[node.kind]}</p>}
      <dl class="kv">
        {node.detail && [<dt>detail</dt>, <dd>{node.detail}</dd>]}
        {node.estimate >= 0 && [<dt>estimate</dt>, <dd>{n(node.estimate)} rows</dd>]}
        {actual !== null && [<dt>actual</dt>, <dd>{n(actual)} rows{state.done ? "" : " so far"}</dd>]}
      </dl>
      <div><span class="note">output columns</span><ul class="cols">{node.columns.map((c: Json) => <li><code>{c.name}</code> <span class="faint">{c.type}</span></li>)}</ul></div>
      {ops.length > 0 && (
        <div class="oplinks"><span class="note">operators</span><ul>{ops.map((o) => (
          <li><button class="link" onClick={() => { q.sub = "program"; q.sel = { kind: "op", id: o.id }; update(); }}>{o.id} {o.label}</button></li>
        ))}</ul></div>
      )}
    </div>
  );
}

export function PlanTree({ q, tree, state, changed, linkOps = false, tag }: { q: Query; tree: Json; state?: Json; changed?: Set<string>; linkOps?: boolean; tag: string }) {
  const walk = (node: Json, path: string): Json => {
    const key = `${tag}:${node.id}`;
    const open = q.sel?.kind === "node" && q.sel.id === key;
    const actual = linkOps && state ? actualRows(state, node.id) : null;
    return (
      <div class="tree" data-tour="plan">
        <div class="tcell">
          <div class="tcard">
            <div class={`tnode ${open ? "sel" : ""} ${changed?.has(path) ? "changed" : ""}`} onClick={() => { q.sel = open ? null : { kind: "node", id: key }; update(); }}>
              <div class="ty"><span>{node.kind}</span>{node.estimate >= 0 && <span class="faint" style="font-weight:400;font-size:11px">≈{n(node.estimate)}</span>}</div>
              <div class="dt">{node.detail}</div>
              {actual !== null && <div class="est">{n(actual)} rows</div>}
            </div>
            {open && <NodePanel q={q} node={node} state={state} linkOps={linkOps} />}
          </div>
        </div>
        {node.children.length > 0 && <div class="tkids">{node.children.map((c: Json, i: number) => walk(c, `${path}.${i}`))}</div>}
      </div>
    );
  };
  return <div class="scroll tree-scroll">{walk(tree, "0")}</div>;
}

/** Paths ("0.1.0") whose subtree differs between two trees: what a rule changed. */
function diff(a: Json, b: Json, path = "0", out = new Set<string>()): Set<string> {
  const sig = (t: Json) => `${t.kind}|${t.detail}|${t.columns.map((c: Json) => c.name).join(",")}`;
  if (!a || sig(a) !== sig(b) || a.children.length !== b.children.length) out.add(path);
  b.children.forEach((c: Json, i: number) => diff(a?.children[i], c, `${path}.${i}`, out));
  return out;
}

/** A stage of the plan: the analyzed plan, then the plan after each rule (run to a
 * fixpoint). A rule that changed nothing leaves the plan as it was. */
interface Stage {
  label: string;
  rule: string | null;
  passes: number;
  changes: number;
  ms: number;
  tree: Json;
}

function stages(q: Query): Stage[] {
  const out: Stage[] = [{ label: "analyzed", rule: null, passes: 0, changes: 0, ms: 0, tree: q.plans.logical }];
  const timings: Json[] = q.plans.timings;
  const steps: Json[] = q.plans.steps;
  for (const t of timings) {
    let st = out[out.length - 1]!;
    if (st.rule !== t.rule) {
      st = { label: t.rule, rule: t.rule, passes: 0, changes: 0, ms: 0, tree: st.tree };
      out.push(st);
    }
    st.passes++;
    st.ms += t.end - t.start;
    if (t.changed) {
      st.changes++;
      st.tree = steps.find((x) => x.rule === t.rule && x.iteration === t.iteration)?.tree ?? st.tree;
    }
  }
  // the last stage is the optimized plan, whose nodes link to the operators
  out[out.length - 1]!.tree = q.plans.optimized;
  return out;
}

/** The plan, stage by stage: the analyzed plan, then each optimizer rule in order. */
export function PlanView({ q, state }: { q: Query; state: Json }) {
  const all = stages(q);
  const last = all.length - 1;
  const k = q.planStep === null ? last : Math.min(q.planStep, last);
  const st = all[k]!;
  const prev = k > 0 ? all[k - 1]! : null;
  const applied = all.filter((x) => x.changes > 0).length;
  const go = (i: number) => { q.planStep = i === last ? null : i; q.sel = null; update(); };
  return (
    <div class="stack">
      <div class="pstages" data-tour="stages">
        {all.map((x, i) => [
          i > 0 && <span class="parrow">→</span>,
          <button class={`pstage ${i === k ? "on" : ""} ${x.rule && !x.changes ? "noop" : ""}`} onClick={() => go(i)}
            title={x.rule === null ? "the plan the analyzer built from the SQL" : x.changes ? `changed the plan in ${x.changes} of ${x.passes} passes, ${ms(x.ms)}` : `ran once (${ms(x.ms)}) and found nothing to change`}>
            <span class="pn">{i === 0 ? "0" : i}</span>
            <span class="pl">{x.label}</span>
            {x.rule && (x.changes ? <span class="pc">{x.changes > 1 ? `${x.changes}×` : "✓"}</span> : <span class="pc">no change</span>)}
          </button>,
        ])}
      </div>
      <p class="note">
        {st.rule === null
          ? <>The plan the analyzer built from the SQL, before any optimizer rule ran: names resolved, every expression typed. Then each rule runs, in order, to a fixpoint: {applied} of {last} changed the plan.</>
          : st.changes
            ? <>After <b>{st.rule}</b>: it changed the plan in {st.changes} pass{st.changes === 1 ? "" : "es"}{st.passes > st.changes ? ", then found nothing more to do" : ""}. Dashed nodes differ from the stage before.</>
            : <><b>{st.rule}</b> found nothing to change: the plan is the same as the stage before.</>}
        {k === last && <> This is the optimized plan the program is compiled from: with the query run, each node shows the rows it produced; click one to see its operators.</>}
        {" "}≈ is the optimizer's row estimate from the catalog's statistics.
      </p>
      <PlanTree q={q} tree={st.tree} state={state} linkOps={k === last} changed={prev && st.changes ? diff(prev.tree, st.tree) : undefined} tag={k === last ? "o" : `s${k}`} />
    </div>
  );
}
