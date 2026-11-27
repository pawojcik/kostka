// The history: the steps of an execution (a scheduler's decisions and the units run on
// nodes), each with its messages. The banner says what the step shown did and lists
// its messages; the Steps view lists them all (filtered by the engine's kinds of
// step; click one to go to it).

import { S, update, type Json, type Query } from "./store.ts";
import { ms, n } from "./format.ts";
import { viewsOf } from "./engines/index.ts";

/** The engine's kinds of step (Session.info: who takes them, how they are named). */
function kindsOf(engine: string): Json[] {
  return S.info?.engines.find((e: Json) => e.name === engine)?.steps ?? [];
}

export function KindChip({ engine, kind }: { engine: string; kind: string }) {
  const k = kindsOf(engine).find((x) => x.kind === kind);
  return <span class={`ev ev-${k?.by ?? "node"} ${k?.bad ? "bad" : ""}`} title={k?.doc}>{k?.label ?? kind}</span>;
}

function Messages({ e, limit = 40 }: { e: Json; limit?: number }) {
  const ms_: Json[] = e.messages ?? [];
  if (!ms_.length) return null;
  return (
    <ul class="msgs">
      {ms_.slice(0, limit).map((m) => (
        <li class={m.dir === "command" ? "cmd" : m.dir === "lost" ? "lost" : "evt"} title={m.dir === "lost" ? "sent, but never delivered: its worker was gone" : undefined}>
          <span class="mono faint">{m.from === "coordinator" ? "coord" : m.from} → {m.to === "coordinator" ? "coord" : m.to}</span>
          <span>{m.text}</span>
        </li>
      ))}
      {ms_.length > limit && <li class="faint">… and {n(ms_.length - limit)} more</li>}
    </ul>
  );
}

/** The step the views show the query right after. */
export function EventBanner({ q, state }: { q: Query; state: Json }) {
  const e = state.event;
  if (!e) return null;
  return (
    <div class="evbanner stack" data-tour="banner">
      <div class="row">
        <span class="mono faint">#{e.seq}</span>
        <KindChip engine={state.engine} kind={e.kind} />
        <span class="grow">{e.text}</span>
        <span class="mono faint">{ms(e.ms)}</span>
        {q.sub !== "events" && <button class="link" onClick={() => { q.sub = "events"; update(); }}>all steps</button>}
      </div>
      <Messages e={e} limit={8} />
    </div>
  );
}

export function EventsView({ q, state, go }: { q: Query; state: Json; go: (at: number) => void }) {
  const counts = new Map<string, number>();
  for (const e of q.events) counts.set(e.kind, (counts.get(e.kind) ?? 0) + 1);
  const shown = q.events.filter((e) => !q.hidden.has(e.kind));
  const toggle = (k: string) => { if (q.hidden.has(k)) q.hidden.delete(k); else q.hidden.add(k); update(); };
  const kinds = kindsOf(state.engine);
  const v = viewsOf(state.engine);
  const only = (by: string) => { q.hidden = new Set(kinds.filter((k) => k.by !== by).map((k) => k.kind)); update(); };
  const at = state.event?.seq ?? 0;
  const LIMIT = 2000;
  const start = Math.max(0, shown.findIndex((e) => e.seq >= at) - LIMIT / 2);
  return (
    <div class="stack">
      <p class="note">The coordinator and the workers talk only by messages: the coordinator sends <b>commands</b> (AddSplits, ReleaseBuffer), the workers answer with <b>events</b> (SplitDone, SplitFailed); the infrastructure says when a worker is gone (WorkerGone). Every step groups the messages of one decision of the coordinator, or of one split a worker ran; a message a gone worker sent that never arrived is shown struck through. The views show the query right after the step chosen here or with the history slider.</p>
      <div class="row evfilter">
        {kinds.filter((k) => counts.has(k.kind)).map(({ kind: k }) => (
          <label class={q.hidden.has(k) ? "off" : ""}><input type="checkbox" checked={!q.hidden.has(k)} onChange={() => toggle(k)} /><KindChip engine={state.engine} kind={k} /> <span class="faint">{n(counts.get(k))}</span></label>
        ))}
        <span class="grow" />
        <button class="btn small" onClick={() => { q.hidden = new Set(); update(); }}>All</button>
        <button class="btn small" onClick={() => only("scheduler")}>Coordinator</button>
        <button class="btn small" onClick={() => only("node")}>Workers</button>
      </div>
      <ul class="evlist">
        {shown.slice(start, start + LIMIT).map((e) => (
          <li class={`${e.seq === at ? "on" : ""} ${e.seq > at ? "later" : ""}`} onClick={() => go(e.seq)}>
            <div class="row">
              <span class="mono faint">#{e.seq}</span>
              <span class="mono faint">{ms(e.ms).padStart(9)}</span>
              <KindChip engine={state.engine} kind={e.kind} />
              {e.node !== null && kinds.find((k) => k.kind === e.kind)?.by === "node" ? <span class={`wtag wk-${e.node % 8}`}>{v.nodeName(e.node)}</span> : <span class="wtag coord">coord</span>}
              <span class="grow">{e.text}</span>
              <span class="faint">{e.messages.length ? `${n(e.messages.length)} msg` : ""}</span>
            </div>
            {e.seq === at && <Messages e={e} />}
          </li>
        ))}
      </ul>
      {shown.length > LIMIT && <p class="note">{n(LIMIT)} of {n(shown.length)} steps around the one shown.</p>}
    </div>
  );
}
