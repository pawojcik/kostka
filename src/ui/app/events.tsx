// The history: the coordinator's and the workers' messages, grouped into steps. The
// banner says what the step shown did and lists its messages; the Steps view lists
// them all (filtered by kind; click one to go to it).

import { update, type Json, type Query } from "./store.ts";
import { ms, n } from "./format.ts";

/** Step kinds: who decides, and how the page names them. */
export const KINDS: [kind: string, who: "coordinator" | "worker" | "failure", label: string, doc: string][] = [
  ["start", "coordinator", "submitted", "the coordinator gets the plan and the workers; every worker has every pipeline; nothing ran yet"],
  ["plan", "coordinator", "splits made", "a pipeline is ready: the coordinator makes its splits, from data files or by cutting its input buffer's outputs, and gives each partition an owner"],
  ["assign", "coordinator", "splits assigned", "the coordinator sends a worker splits for its queue (AddSplits, at most 2 queued per pipeline), or moves a partition that has not started to an idle worker"],
  ["process", "worker", "split processed", "a worker ran a split: the pieces it wrote, hash tables it built or reused, result rows, and SplitDone"],
  ["split_done", "coordinator", "split reported", "the coordinator handles a SplitDone: it records the split's outputs, filter keys and result rows, and refills the worker's queue"],
  ["split_failed", "coordinator", "split failed", "the coordinator handles a SplitFailed: a piece the split read was on a worker that is gone, so it runs again once that output is made again"],
  ["enough", "coordinator", "enough rows", "a Limit's gather buffer holds enough rows: the coordinator skips the producer's pending splits"],
  ["worker_gone", "failure", "worker gone", "a worker is gone: its queued splits go back to pending, its outputs are lost, its partitions get new owners, and the splits that made lost outputs still needed run again"],
  ["pipeline_done", "coordinator", "pipeline done", "every split is done: its output buffer is sealed, runtime filter keys merged, buffers nobody reads any more released, and pipelines waiting for it may start"],
  ["done", "coordinator", "done", "the query is done"],
  ["failed", "coordinator", "failed", "a split failed, or every worker is gone, so the query failed"],
]
const BY_KIND = new Map(KINDS.map((k) => [k[0], k]));

export function KindChip({ kind }: { kind: string }) {
  const k = BY_KIND.get(kind);
  return <span class={`ev ev-${k?.[1] ?? "worker"} ${kind === "failed" || kind === "worker_gone" || kind === "split_failed" ? "bad" : ""}`} title={k?.[3]}>{k?.[2] ?? kind}</span>;
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
        <KindChip kind={e.kind} />
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
  const only = (who: string) => { q.hidden = new Set(KINDS.filter((k) => k[1] !== who).map((k) => k[0])); update(); };
  const at = state.event?.seq ?? 0;
  const LIMIT = 2000;
  const start = Math.max(0, shown.findIndex((e) => e.seq >= at) - LIMIT / 2);
  return (
    <div class="stack">
      <p class="note">The coordinator and the workers talk only by messages: the coordinator sends <b>commands</b> (AddSplits, ReleaseBuffer), the workers answer with <b>events</b> (SplitDone, SplitFailed); the infrastructure says when a worker is gone (WorkerGone). Every step groups the messages of one decision of the coordinator, or of one split a worker ran; a message a gone worker sent that never arrived is shown struck through. The views show the query right after the step chosen here or with the history slider.</p>
      <div class="row evfilter">
        {KINDS.filter((k) => counts.has(k[0])).map(([k]) => (
          <label class={q.hidden.has(k) ? "off" : ""}><input type="checkbox" checked={!q.hidden.has(k)} onChange={() => toggle(k)} /><KindChip kind={k} /> <span class="faint">{n(counts.get(k))}</span></label>
        ))}
        <span class="grow" />
        <button class="btn small" onClick={() => { q.hidden = new Set(); update(); }}>All</button>
        <button class="btn small" onClick={() => only("coordinator")}>Coordinator</button>
        <button class="btn small" onClick={() => only("worker")}>Workers</button>
      </div>
      <ul class="evlist">
        {shown.slice(start, start + LIMIT).map((e) => (
          <li class={`${e.seq === at ? "on" : ""} ${e.seq > at ? "later" : ""}`} onClick={() => go(e.seq)}>
            <div class="row">
              <span class="mono faint">#{e.seq}</span>
              <span class="mono faint">{ms(e.ms).padStart(9)}</span>
              <KindChip kind={e.kind} />
              {e.worker !== null && e.kind === "process" ? <span class={`wtag wk-${e.worker % 8}`}>W{e.worker}</span> : <span class="wtag coord">coord</span>}
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
