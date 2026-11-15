// Workers: a timeline lane per worker with a rectangle per split it ran, colored by
// pipeline, on the simulated clock; then each worker's time by category, its queues,
// the hash tables it caches, what it holds in memory and on disk, a button that
// stops it (a failure), and the coordinator's log.

import { call } from "../rpc.ts";
import { S, toast, update, type Json, type Query } from "./store.ts";
import { bytes, ms, n, pc } from "./format.ts";
import { KV, TimeBar } from "./widgets.tsx";
import { KindChip } from "./events.tsx";

const KINDS: Record<string, string> = { output: "output pieces", hash: "hash table", groups: "aggregate groups", sort: "sort buffer", buffer: "buffers" };

/** Stops worker `w` now: the simulation sends the coordinator WorkerGone. Only at the
 * latest step, while the query runs. */
async function stop(q: Query, w: number): Promise<void> {
  if (S.busy) return;
  S.busy = true;
  q.pending = `Stopping W${w}`;
  update();
  try {
    const r = await call("stopWorker", q.key, w, q.events.length);
    q.events.push(...r.events);
    q.hist.set(r.at, r.state);
    q.state = r.state;
    q.at = null;
  } catch (e) {
    toast((e as Error).message);
  } finally {
    S.busy = false;
    q.pending = null;
    update();
  }
}

export function StopButton({ q, state, w }: { q: Query; state: Json; w: number }) {
  const gone = state.members?.[w]?.status === "gone";
  if (gone) return null;
  const latest = q.at === null;
  const over = state.done || !!state.error;
  const why = over ? "the query is over" : !latest ? "go to the latest step first: a failure happens now, not in the past" : `stop W${w} now, with everything it holds: the coordinator gets WorkerGone and recovers`;
  return (
    <button class="btn small danger" data-tour={`stop-${w}`} disabled={over || !latest || S.busy} title={why} onClick={(e) => { e.stopPropagation(); void stop(q, w); }}>Stop</button>
  );
}

export function Timeline({ q, state, compact = false }: { q: Query; state: Json; compact?: boolean }) {
  const end = Math.max(state.elapsed_ms ?? 0, 1e-6);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => f * end);
  return (
    <div data-tour="timeline" class={`tline ${compact ? "compact" : ""}`}>
      {state.workers.map((w: Json) => {
        const gone = state.members?.[w.worker]?.status === "gone";
        return (
          <div class={`tlane ${gone ? "gone" : ""}`}>
            {!compact && <span class="tll mono">W{w.worker}{gone ? " ✕" : ""}</span>}
            <div class="tlt">
              {state.units.filter((u: Json) => u.worker === w.worker).map((u: Json) => (
                <i class={`${pc(u.pipeline)} ${u.failed ? "failed" : ""} ${u.attempt > 1 ? "again" : ""} ${q.sel?.kind === "split" && q.sel.id === u.split ? "hl" : ""}`}
                  style={`left:${(100 * u.start_ms) / end}%;width:max(2px, ${(100 * (u.end_ms - u.start_ms)) / end}%)`}
                  title={`P${u.pipeline} on W${u.worker}: split ${u.split}${u.attempt > 1 ? ` (attempt ${u.attempt})` : ""} (${u.label})${u.failed ? `\nfailed: ${u.failed}` : ""}\n${ms(u.start_ms)} → ${ms(u.end_ms)}\n${u.ops.map((o: Json, i: number) => `${i}: ${n(o.rows_in)} → ${n(o.rows_out)} rows`).join("\n")}`}
                  onClick={() => { q.sel = { kind: "split", id: u.split }; update(); }}>
                  {compact ? "" : `P${u.pipeline}`}
                </i>
              ))}
              <span class="tnow" style={`left:${(100 * (w.clock_ms ?? 0)) / end}%`} />
            </div>
          </div>
        );
      })}
      {!compact && (
        <div class="tlane axis"><span /><div class="tlt ticks">{ticks.map((t) => <span style={`left:${(100 * t) / end}%`}>{ms(t)}</span>)}</div></div>
      )}
    </div>
  );
}

function Strip({ items, cap }: { items: Json[]; cap: number }) {
  return (
    <div class="mstrip">
      <div class="mitems">
        {items.map((it) => <i class={`${it.pipeline >= 0 ? pc(it.pipeline) : "pc-x"} k-${it.kind}`} style={`width:max(3px, ${(100 * it.bytes) / Math.max(cap, 1)}%)`} title={`${it.label}: ${it.count > 1 ? `${n(it.count)} × ` : ""}${KINDS[it.kind] ?? it.kind}, ${bytes(it.bytes)}`} />)}
      </div>
    </div>
  );
}

export function WorkersView({ q, state }: { q: Query; state: Json }) {
  const busiest = Math.max(1e-6, ...state.workers.map((w: Json) => w.busy_ms));
  const elapsed = state.elapsed_ms ?? 0;
  return (
    <div class="stack">
      <section class="panel stack">
        <div class="row"><h4 class="grow">Timeline</h4><span class="note">simulated time: {ms(elapsed)} · {n(state.units_run)} units · step {n(state.at)} of {n(state.events - 1)}</span></div>
        <Timeline q={q} state={state} />
        <p class="note">One lane per worker; a rectangle per split it ran, colored by pipeline (striped: it ran again after a failure; outlined in red: it failed). The line is the worker's clock: when it is free again. Times are simulated: CPU {state.cluster.cpu === "model" ? "from the cost model" : "as measured in this browser"}, IO from bytes and the IO profile.</p>
      </section>
      <div class="wgrid" data-tour="workers">
        {state.workers.map((w: Json) => {
          const tm = { ...w.time_ms, idle: Math.max(0, elapsed - w.busy_ms) };
          const gone = state.members?.[w.worker]?.status === "gone";
          return (
            <div data-tour={`wcard-${w.worker}`} class={`wcard ${gone ? "gone" : ""}`}>
              <div class="row"><b class="grow">W{w.worker}</b>{gone ? <span class="chip bad">gone</span> : <StopButton q={q} state={state} w={w.worker} />}</div>
              <span class="note">{n(w.runs)} splits · busy {ms(w.busy_ms)}</span>
              {w.queued.length > 0 && <div class="wl">queued: {w.queued.map((x: Json) => `P${x.pipeline} ${x.splits.map((s: Json) => `${s.id}${s.attempt > 1 ? ` (attempt ${s.attempt})` : ""}`).join(", ")}`).join(" · ")}</div>}
              {w.cache.length > 0 && <div class="wl">cached hash tables: {w.cache.map((c: Json) => `B${c.buffer}${/:p(\d+)/.exec(c.key) ? ` #${/:p(\d+)/.exec(c.key)![1]}` : ""}`).join(", ")}</div>}
              <TimeBar tm={tm} scale={Math.max(elapsed, busiest)} legend={false} />
              <div class="wl">memory {bytes(w.memory.used)} of {bytes(w.memory.capacity)} · peak {bytes(w.memory.peak)}</div>
              <Strip items={w.memory.items} cap={w.memory.capacity} />
              {(w.disk.peak > 0 || w.spills > 0) && <>
                <div class="wl">disk {bytes(w.disk.used)} of {bytes(w.disk.capacity)} · peak {bytes(w.disk.peak)} · {n(w.spills)} spills</div>
                <Strip items={w.disk.items} cap={w.disk.capacity} />
              </>}
              <KV pairs={[["storage read", bytes(w.bytes.storage_read)], ["network in / out", `${bytes(w.bytes.network_in)} / ${bytes(w.bytes.network_out)}`]]} />
            </div>
          );
        })}
      </div>
      <div class="tlegend">
        {Object.entries(KINDS).map(([k, l]) => <span><i class={`sw pc-x k-${k}`} />{l}</span>)}
        <span class="note">Memory strips: each item as wide as its bytes, colored by the pipeline holding it. Pieces spill to disk, oldest first, when something new doesn't fit. The Stop button is a failure: the worker is gone with its queues, its pieces and its hash tables; it is not replaced.</span>
      </div>
      <section class="panel stack">
        <h4>Events so far</h4>
        <ul class="clog">{state.log.map(([t, text, kind, seq]: [number, string, string, number]) => <li class={seq === state.at ? "on" : ""}><span class="faint mono">{ms(t).padStart(9)}</span> <KindChip kind={kind} /> {text}</li>)}</ul>
      </section>
    </div>
  );
}
