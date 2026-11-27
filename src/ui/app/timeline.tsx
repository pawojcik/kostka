// The timeline: a row per node and, inside it, a lane per core, on the simulated
// clock. Each unit of work is a rectangle in its core's lane, drawn as its engine says
// (engines/views.ts); time it spent waiting for the node's IO channel is hatched. The
// line is the core's clock: when it is free again. Also the Stop button, a failure:
// the node stops now, with everything on it.

import { call } from "../rpc.ts";
import { S, toast, update, type Json, type Query } from "./store.ts";
import { ms } from "./format.ts";
import { viewsOf } from "./engines/index.ts";

/** Stops node `node` now: its engine's scheduler hears of it at once. Only at the
 * latest step, while the query runs. */
async function stop(q: Query, node: number, name: string): Promise<void> {
  if (S.busy) return;
  S.busy = true;
  q.pending = `Stopping ${name}`;
  update();
  try {
    const r = await call("stopNode", q.key, node, q.events.length);
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
  if (state.nodes?.[w]?.status !== "up") return null;
  const v = viewsOf(state.engine);
  const name = v.nodeName(w);
  const latest = q.at === null;
  const over = state.done || !!state.error;
  const why = over ? "the query is over" : !latest ? "go to the latest step first: a failure happens now, not in the past" : `stop ${name} now, with everything it holds: the scheduler hears that it is gone and recovers`;
  return (
    <button class="btn small danger" data-tour={`stop-${w}`} disabled={over || !latest || S.busy} title={why} onClick={(e) => { e.stopPropagation(); void stop(q, w, name); }}>Stop</button>
  );
}

export function Timeline({ q, state, compact = false }: { q: Query; state: Json; compact?: boolean }) {
  const v = viewsOf(state.engine);
  const end = Math.max(state.elapsed_ms ?? 0, 1e-6);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => f * end);
  const pct = (x: number) => (100 * x) / end;
  return (
    <div data-tour="timeline" class={`tline ${compact ? "compact" : ""}`}>
      {state.nodes.flatMap((nd: Json) => {
        const gone = nd.status === "gone";
        return nd.cores.map((core: Json, c: number) => (
          <div class={`tlane ${gone ? "gone" : ""}`}>
            {!compact && <span class="tll mono">{c === 0 ? `${v.nodeName(nd.node)}${gone ? " ✕" : ""}` : ""}{nd.cores.length > 1 ? <span class="faint">·{c}</span> : null}</span>}
            <div class="tlt">
              {state.units.filter((u: Json) => u.node === nd.node && u.core === c).map((u: Json) => {
                const look = v.unit(u, q);
                const span = Math.max(u.end_ms - u.start_ms, 1e-9);
                return (
                  <i class={`${look.cls} ${look.on ? "hl" : ""}`} style={`left:${pct(u.start_ms)}%;width:max(2px, ${pct(u.end_ms - u.start_ms)}%)`} title={look.title} onClick={look.select}>
                    {u.segments.filter((s: Json) => s[0] === "wait").map((s: Json) => (
                      <b class="wait" style={`left:${(100 * (s[1] - u.start_ms)) / span}%;width:${(100 * (s[2] - s[1])) / span}%`} />
                    ))}
                    {compact ? "" : look.text}
                  </i>
                );
              })}
              <span class="tnow" style={`left:${pct(core.clock_ms ?? 0)}%`} />
            </div>
          </div>
        ));
      })}
      {!compact && (
        <div class="tlane axis"><span /><div class="tlt ticks">{ticks.map((t) => <span style={`left:${pct(t)}%`}>{ms(t)}</span>)}</div></div>
      )}
    </div>
  );
}
