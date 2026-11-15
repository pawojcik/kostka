// The query state: what the coordinator knows right after the step shown, drawn
// rather than listed. The cluster (each worker up or gone, the splits queued on it,
// the hash tables it caches), each pipeline's splits as a grid of squares (color:
// the worker, fill: the status), each output buffer as partitions × outputs (who
// holds what, and what was lost or released), and the runtime filters. Hovering a
// split or an output lights up its lineage: the outputs a split read and wrote, the
// split that made an output and the splits that read it.

import { useState } from "preact/hooks";
import type { Json, Query } from "./store.ts";
import { bytes, n, pc } from "./format.ts";
import { Chip } from "./widgets.tsx";
import { StopButton } from "./workers.tsx";

/** What the pointer is on: a split, or an output (buffer, split, partition). */
type Hover = { split: number } | { output: [number, number, number] } | null;

/** The splits and outputs to light up for what is hovered. */
interface Lit {
  splits: Set<number>;
  outputs: Set<string>;
}

const okey = (b: number, s: number, p: number) => `${b}:${s}:${p}`;

function lineage(state: Json, h: Hover): Lit {
  const lit: Lit = { splits: new Set(), outputs: new Set() };
  if (!h) return lit;
  const byId = new Map<number, Json>(state.splits.map((s: Json) => [s.id, s]));
  const readers = (b: number, split: number, part: number) =>
    state.splits.filter((s: Json) => s.buffer === b && s.reads.some(([x, p]: number[]) => x === split && p === part)).map((s: Json) => s.id);
  if ("split" in h) {
    const s = byId.get(h.split);
    if (!s) return lit;
    lit.splits.add(s.id);
    // what it read, and who made that
    for (const [x, p] of s.reads) {
      lit.outputs.add(okey(s.buffer, x, p));
      lit.splits.add(x);
    }
    // what it wrote, and who read that
    for (const b of state.buffers) {
      for (const o of b.outputs) {
        if (o.split !== s.id) continue;
        lit.outputs.add(okey(b.index, o.split, o.partition));
        for (const r of readers(b.index, o.split, o.partition)) lit.splits.add(r);
      }
    }
  } else {
    const [b, x, p] = h.output;
    lit.outputs.add(okey(b, x, p));
    lit.splits.add(x);
    for (const r of readers(b, x, p)) lit.splits.add(r);
  }
  return lit;
}

/** The splits the step shown is about: its own, and those named in its commands. */
function touched(state: Json): Set<number> {
  const e = state.event;
  const out = new Set<number>();
  if (!e) return out;
  if (e.split !== null) out.add(e.split);
  for (const m of e.messages ?? []) if (m.type === "AddSplits") for (const x of m.text.matchAll(/split (\d+)/g)) out.add(Number(x[1]));
  return out;
}

const STATUS_DOC: Record<string, string> = {
  pending: "not given out yet",
  blocked: "pending, but an output it needs is not there (lost, or being made again)",
  queued: "queued on a worker (the coordinator sent AddSplits)",
  ran: "the worker ran it; its SplitDone is on the way",
  done: "done: the coordinator has its SplitDone",
  skipped: "skipped: the Limit had enough rows",
  failed: "failed",
};

function splitClass(s: Json): string {
  const st = s.status === "pending" && s.blocked ? "blocked" : s.status === "queued" && s.ran ? "ran" : s.status;
  return `sq st-${st} ${s.worker !== null && s.status !== "pending" ? `wk-${s.worker % 8}` : ""}`;
}

function Square({ s, lit, hot, next, hover }: { s: Json; lit: Lit; hot: boolean; next: boolean; hover: (h: Hover) => void }) {
  const st = s.status === "pending" && s.blocked ? "blocked" : s.status === "queued" && s.ran ? "ran" : s.status;
  const title = `split ${s.id}: ${s.label}\n${n(s.rows)} rows, ${bytes(s.bytes)}${s.reads.length ? `, reads ${n(s.reads.length)} output${s.reads.length === 1 ? "" : "s"}` : ""}\n${STATUS_DOC[st] ?? st}${s.worker !== null ? ` · W${s.worker}` : ""}${s.attempt > 1 ? ` · attempt ${s.attempt}` : ""}${s.reason ? `\n${s.reason}` : ""}${s.blocked ? `\nblocked: ${s.blocked}` : ""}`;
  return (
    <i class={`${splitClass(s)} ${lit.splits.has(s.id) ? "lit" : ""} ${hot ? "hot" : ""} ${next ? "next" : ""}`} title={title}
      onMouseEnter={() => hover({ split: s.id })} onMouseLeave={() => hover(null)}>
      {s.attempt > 1 && <sup>{s.attempt}</sup>}
    </i>
  );
}

function Cluster({ q, state }: { q: Query; state: Json }) {
  const k = state.queue_depth ?? 2;
  return (
    <div class="qcluster">
      {state.workers.map((w: Json) => {
        const gone = state.members[w.worker]?.status === "gone";
        const held = state.buffers.reduce((a: number, b: Json) => a + b.outputs.filter((o: Json) => o.worker === w.worker && o.status === "available").length, 0);
        const owns = new Set<number>();
        for (const p of state.pipelines) for (const [part, o] of Object.entries(p.owners)) if (o === w.worker) owns.add(Number(part));
        return (
          <div class={`qworker wk-${w.worker % 8} ${gone ? "gone" : ""}`}>
            <div class="row"><b class="qwname">W{w.worker}</b><Chip kind={gone ? "bad" : "ok"}>{gone ? "gone" : "up"}</Chip><span class="grow" /><StopButton q={q} state={state} w={w.worker} /></div>
            <div class="qqueues">
              {state.pipelines.filter((p: Json) => p.status === "running").map((p: Json) => {
                const queued = state.splits.filter((s: Json) => s.pipeline === p.index && s.status === "queued" && s.worker === w.worker);
                return (
                  <div class="qqueue" title={`the coordinator has ${queued.length} of P${p.index}'s splits queued on W${w.worker} (at most ${k})`}>
                    <i class={`sw ${pc(p.index)}`} /><span class="mono faint">P{p.index}</span>
                    {Array.from({ length: k }, (_, i) => {
                      const s = queued[i];
                      return (
                        <span class={`qslot ${s ? `full ${s.ran ? "ran" : ""} ${s.attempt > 1 ? "again" : ""}` : ""}`}
                          title={s ? `split ${s.id}: ${s.label}${s.attempt > 1 ? `\nattempt ${s.attempt}: ${s.reason ?? "it runs again"}` : ""}${s.ran ? "\nit ran; SplitDone is on the way" : ""}` : "an empty slot"}>
                          {s ? s.id : ""}{s && s.attempt > 1 && <sup>{s.attempt}</sup>}
                        </span>
                      );
                    })}
                  </div>
                );
              })}
            </div>
            <div class="qfacts">
              <span title="outputs the coordinator knows this worker holds">{gone ? "lost " : ""}{n(gone ? state.buffers.reduce((a: number, b: Json) => a + b.outputs.filter((o: Json) => o.worker === w.worker && o.status === "lost").length, 0) : held)} outputs</span>
              {owns.size > 0 && <span title="partitions it owns, in any partitioning">owns {[...owns].sort((a, b) => a - b).map((x) => `#${x}`).join(" ")}</span>}
            </div>
            {w.cache?.length > 0 && <div class="qcache">{w.cache.map((c: Json) => <span class="qtable" title={`a hash table built from B${c.buffer} (${c.key}), reused ${n(c.hits)} times`}>⌗ B{c.buffer}{/:p(\d+)/.exec(c.key)?.[1] !== undefined ? ` #${/:p(\d+)/.exec(c.key)![1]}` : ""}{c.hits ? ` ×${c.hits + 1}` : ""}</span>)}</div>}
          </div>
        );
      })}
    </div>
  );
}

export function Bar({ p }: { p: Json }) {
  const c = p.splits;
  const total = Math.max(1, c.total);
  const parts: [string, number, string][] = [["done", c.done, "done"], ["queued", c.queued, "queued"], ["blocked", c.blocked, "blocked"], ["pending", c.pending - c.blocked, "not given out"], ["skipped", c.skipped, "skipped"], ["failed", c.failed, "failed"]];
  return (
    <div class="qbar" title={parts.filter((x) => x[1]).map(([, v, l]) => `${n(v)} ${l}`).join(", ")}>
      {parts.map(([k, v]) => (v > 0 ? <i class={`qb-${k}`} style={`width:${(100 * v) / total}%`} /> : null))}
    </div>
  );
}

function Pipelines({ state, lit, hover }: { state: Json; lit: Lit; hover: (h: Hover) => void }) {
  const hot = touched(state);
  const next = state.next?.kind === "split" ? state.next.split : null;
  return (
    <div class="stack">
      {state.pipelines.map((p: Json) => {
        const ss = state.splits.filter((s: Json) => s.pipeline === p.index);
        const byPart = new Map<number, Json[]>();
        for (const s of ss) if (s.partition !== null && s.kind === "buffer") byPart.set(s.partition, [...(byPart.get(s.partition) ?? []), s]);
        const chip = p.status === "done" ? "ok" : p.status === "running" ? "run" : "";
        return (
          <div class={`qpipe ${p.status}`}>
            <div class="row">
              <i class={`sw ${pc(p.index)}`} /><b>P{p.index}</b>
              <span class="mono note qlabel">{p.label}</span>
              <span class="grow" />
              {p.completions > 1 && <Chip kind="warn">done {p.completions}×</Chip>}
              {p.splits.again > 0 && <Chip kind="warn">{n(p.splits.again)} ran again</Chip>}
              <Chip kind={chip}>{p.status === "waiting" ? `waits for ${p.depends_on.map((i: number) => `P${i}`).join(", ")}` : p.status}</Chip>
              <span class="note">{n(p.splits.done)}/{n(p.splits.total)}</span>
            </div>
            {ss.length > 0 && <Bar p={p} />}
            {ss.length > 0 && byPart.size === 0 && <div class="sqs">{ss.map((s: Json) => <Square s={s} lit={lit} hot={hot.has(s.id)} next={next === s.id} hover={hover} />)}</div>}
            {byPart.size > 0 && (
              <div class="qparts">
                {[...byPart].sort((a, b) => a[0] - b[0]).map(([part, xs]) => {
                  const owner = p.owners[part];
                  return (
                    <div class="qpart">
                      <span class="mono faint">#{part}</span>
                      {owner !== undefined ? <span class={`wtag wk-${owner % 8} ${state.members[owner]?.status === "gone" ? "gone" : ""}`} title={`W${owner} owns partition #${part}`}>W{owner}</span> : <span />}
                      <div class="sqs">{xs.map((s) => <Square s={s} lit={lit} hot={hot.has(s.id)} next={next === s.id} hover={hover} />)}</div>
                    </div>
                  );
                })}
              </div>
            )}
            {p.status !== "waiting" && ss.length === 0 && <p class="note">No splits: its input is empty.</p>}
            {p.skipped_files.length > 0 && <p class="note">Runtime filters ruled out {n(p.skipped_files.length)} files.</p>}
          </div>
        );
      })}
    </div>
  );
}

const BSTATUS: Record<string, string> = { empty: "", filling: "run", sealed: "ok", released: "skip" };

function Buffers({ state, lit, hover }: { state: Json; lit: Lit; hover: (h: Hover) => void }) {
  if (!state.buffers.length) return <p class="note">This plan has no output buffers: it runs as one pipeline, straight to the result.</p>;
  const producers = new Map<number, Json[]>();
  for (const s of state.splits) producers.set(s.pipeline, [...(producers.get(s.pipeline) ?? []), s]);
  return (
    <div class="qbufs">
      {state.buffers.map((b: Json) => {
        // one column per split of the producer that wrote anything to this buffer
        const cols = [...new Set<number>(b.outputs.map((o: Json) => o.split))].sort((x, y) => x - y);
        const at = new Map<string, Json>(b.outputs.map((o: Json) => [`${o.split}:${o.partition}`, o]));
        const max = Math.max(1, ...b.outputs.map((o: Json) => o.rows));
        const lost = b.outputs.filter((o: Json) => o.status === "lost").length;
        return (
          <div class="qbuf">
            <div class="row">
              <b>B{b.index}</b><span class="note">{b.mode}{b.partitions > 1 ? ` · ${b.partitions} partitions` : ""} · {b.label}</span>
              <span class="grow" />
              {b.enough !== null && b.enough !== undefined && <Chip kind="warn" >enough at {n(b.enough)} rows</Chip>}
              {lost > 0 && <Chip kind="bad">{n(lost)} lost</Chip>}
              <Chip kind={BSTATUS[b.status]}>{b.status}</Chip>
              <span class="note">P{b.producer} → {b.consumers.map((c: number) => `P${c}`).join(", ") || "result"}</span>
            </div>
            {cols.length === 0 ? <p class="note">Nothing written yet.</p> : (
              <div class="qgrid-scroll">
                <table class="qgrid">
                  <thead><tr><th />{cols.map((c) => <th class="mono faint" title={`split ${c} of P${b.producer}`}>{c}</th>)}</tr></thead>
                  <tbody>
                    {Array.from({ length: b.partitions }, (_, part) => (
                      <tr>
                        <th class="mono faint">#{part}{b.owners[part] !== undefined && <span class={`wdot wk-${b.owners[part] % 8}`} title={`owner W${b.owners[part]}`} />}</th>
                        {cols.map((c) => {
                          const o = at.get(`${c}:${part}`);
                          if (!o) return <td />;
                          const k = okey(b.index, c, part);
                          const size = 0.35 + 0.65 * Math.sqrt(o.rows / max);
                          return (
                            <td>
                              <i class={`qcell wk-${o.worker % 8} os-${o.status} ${o.new ? "new" : ""} ${lit.outputs.has(k) ? "lit" : ""}`} style={`--s:${size}`}
                                title={`output of split ${c} to partition #${part}: ${n(o.rows)} rows, ${bytes(o.bytes)} in ${n(o.pieces)} piece${o.pieces === 1 ? "" : "s"} on W${o.worker}\n${o.status === "lost" ? `lost with W${o.worker}` : o.status}${o.attempt > 1 ? ` · made by attempt ${o.attempt}` : ""}`}
                                onMouseEnter={() => hover({ output: [b.index, c, part] })} onMouseLeave={() => hover(null)} />
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function Filters({ state }: { state: Json }) {
  if (!state.filters.length) return null;
  return (
    <div class="row qfilters">
      {state.filters.map((f: Json) => (
        <div class={`qfilter ${f.keys === null ? "" : "on"}`} title={f.label}>
          <b>RF{f.id}</b> <span class="note">P{f.producer} → P{f.consumer}</span>
          <div class="mono">{f.keys === null ? "no keys yet" : `${n(f.keys)} keys${f.range ? ` in [${f.range[0]}, ${f.range[1]}]` : ""}`}</div>
        </div>
      ))}
    </div>
  );
}

export function QueryStateView({ q, state }: { q: Query; state: Json }) {
  const [h, setH] = useState<Hover>(null);
  const lit = lineage(state, h);
  const gone = state.members.filter((m: Json) => m.status === "gone").length;
  return (
    <div class="stack qstate">
      <p class="note">What the <b>coordinator</b> knows right after step {n(state.at)}, built only from the plan and the events it handled: which workers are up, which splits it gave out and to whom, every <b>output</b> (what a split wrote to a partition, and where it is), the partition owners and the runtime filter keys. A worker that is gone takes its outputs with it: the coordinator marks them <b>lost</b> and runs again the splits that made them, if a reader still needs them. Hover a split or an output for its lineage.</p>
      <section class="panel stack" data-tour="state-cluster">
        <div class="row"><h4 class="grow">Cluster</h4><span class="note">{n(state.members.length - gone)} of {n(state.members.length)} workers up · at most {n(state.queue_depth)} splits queued per worker and pipeline · no new workers are provisioned</span></div>
        <Cluster q={q} state={state} />
      </section>
      <section class="panel stack" data-tour="state-pipes">
        <div class="row"><h4 class="grow">Pipelines and their splits</h4>
          <span class="legend">
            <span><i class="sq st-pending" /> not given out</span><span><i class="sq st-blocked" /> blocked</span><span><i class="sq st-queued wk-0" /> queued</span>
            <span><i class="sq st-ran wk-0" /> ran</span><span><i class="sq st-done wk-0" /> done</span><span><i class="sq st-skipped" /> skipped</span><span><i class="sq st-done wk-0"><sup>2</sup></i> attempt</span><span><i class="sq hot" /> this step</span>
          </span>
        </div>
        <Pipelines state={state} lit={lit} hover={setH} />
      </section>
      <section class="panel stack" data-tour="state-buffers">
        <div class="row"><h4 class="grow">Output buffers: partitions × the splits that wrote them</h4>
          <span class="legend"><span><i class="qcell wk-0" /> held (worker color, size by rows)</span><span><i class="qcell wk-0 os-lost" /> lost</span><span><i class="qcell wk-0 os-released" /> released</span><span><i class="wdot wk-0" /> owner</span></span>
        </div>
        <Buffers state={state} lit={lit} hover={setH} />
      </section>
      {state.filters.length > 0 && <section class="panel stack"><h4>Runtime filters</h4><Filters state={state} /></section>}
    </div>
  );
}
