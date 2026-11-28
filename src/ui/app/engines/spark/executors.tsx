// The Spark-style engine's Executors tab: the timeline (a row per node, a lane per
// core: slot i runs on core i), then a card per executor: its task slots, busy (with
// which task) or free; the memory each running task may hold; what its node holds in
// memory and on disk (shuffle files, broadcast tables, task memory, spilled runs);
// the shuffle files and broadcasts it holds, and its failed tasks; and a button that
// stops its node (a failure). Then the driver's log.

import type { Json, Query } from "../../store.ts";
import { bytes, ms, n, pc } from "../../format.ts";
import { KV, TimeBar } from "../../widgets.tsx";
import { KindChip } from "../../events.tsx";
import { StopButton, Timeline } from "../../timeline.tsx";

const KINDS: Record<string, string> = { shuffle: "shuffle files", broadcast: "broadcast tables", task: "task memory", spill: "spilled runs" };

/** The stage a held item belongs to (its label starts with "S<stage>"), or -1. */
const stageOf = (label: string) => Number(/^S(\d+)/.exec(label)?.[1] ?? -1);

function Strip({ items, cap }: { items: Json[]; cap: number }) {
  return (
    <div class="mstrip"><div class="mitems">
      {items.map((it) => <i class={`${stageOf(it.label) >= 0 ? pc(stageOf(it.label)) : "pc-x"} k-${it.kind}`} style={`width:max(3px, ${(100 * it.bytes) / Math.max(cap, 1)}%)`} title={`${it.label}: ${it.count > 1 ? `${n(it.count)} × ` : ""}${KINDS[it.kind] ?? it.kind}, ${bytes(it.bytes)}`} />)}
    </div></div>
  );
}

export function ExecutorsView({ q, state }: { q: Query; state: Json }) {
  const elapsed = state.elapsed_ms ?? 0;
  const busiest = Math.max(1e-6, ...state.nodes.map((w: Json) => w.busy_ms));
  return (
    <div class="stack" data-tour="spark-executors">
      <section class="panel stack">
        <div class="row"><h4 class="grow">Timeline</h4><span class="note">simulated time: {ms(elapsed)} · {n(state.units_run)} tasks · step {n(state.at)} of {n(state.events - 1)}</span></div>
        <Timeline q={q} state={state} />
        <p class="note">A row per executor and a lane per core: slot i runs its tasks on core i, so a node with 2 cores runs 2 tasks side by side. Each rectangle is a task, colored by stage (striped: it ran again; outlined in red: it failed). Hatched: waiting for the node's IO channel, which its cores share. Times are simulated: CPU {state.cluster.cpu === "model" ? "from the cost model" : "as measured in this browser"}, IO from bytes and the IO profile.</p>
      </section>
      <div class="wgrid" data-tour="executors">
        {state.nodes.map((w: Json) => {
          const gone = w.status === "gone";
          const tm = { ...w.time_ms, idle: Math.max(0, elapsed * w.cores.length - w.busy_ms) };
          const files: Json[] = w.shuffle_files ?? [];
          const byShuffle = new Map<number, { count: number; bytes: number }>();
          for (const f of files) {
            const s = byShuffle.get(f.shuffle) ?? { count: 0, bytes: 0 };
            s.count++;
            s.bytes += f.bytes;
            byShuffle.set(f.shuffle, s);
          }
          return (
            <div class={`wcard ${gone ? "gone" : ""}`} data-tour={`ecard-${w.node}`}>
              <div class="row"><b class="grow">Executor {w.node}</b>{gone ? <span class="chip bad">gone</span> : <StopButton q={q} state={state} w={w.node} />}</div>
              <span class="note">{n(w.runs)} tasks · busy {ms(w.busy_ms)} · {n(w.failed_tasks)} failed</span>
              <div class="slots" data-tour={`slots-${w.node}`}>
                {w.cores.map((_: Json, slot: number) => {
                  const t = (w.running ?? []).find((x: Json) => x.slot === slot);
                  return <span class={`slot ${t ? `busy ${pc(t.stage)}` : ""}`} title={t ? `slot ${slot}: task ${t.partition}.${t.attempt} of stage ${t.stage} (TID ${t.tid})${t.ran ? ", ran: its TaskFinished is on the way" : ", waiting for core " + slot}` : `slot ${slot}: free`}>{t ? `S${t.stage}·${t.partition}` : "free"}</span>;
                })}
              </div>
              <TimeBar tm={tm} scale={Math.max(elapsed * w.cores.length, busiest)} legend={false} />
              <div class="wl">memory per running task: {bytes(state.settings.memory_share)} (memory / {w.cores.length} core{w.cores.length === 1 ? "" : "s"})</div>
              <div class="wl">memory {bytes(w.memory.used)} of {bytes(w.memory.capacity)} · peak {bytes(w.memory.peak)}</div>
              <Strip items={w.memory.items} cap={w.memory.capacity} />
              <div class="wl">disk {bytes(w.disk.used)} of {bytes(w.disk.capacity)} · peak {bytes(w.disk.peak)}</div>
              <Strip items={w.disk.items} cap={w.disk.capacity} />
              {byShuffle.size > 0 && <div class="wl">shuffle files: {[...byShuffle].map(([s, x]) => `shuffle ${s}: ${n(x.count)} (${bytes(x.bytes)})`).join(" · ")}</div>}
              {(w.broadcasts ?? []).length > 0 && <div class="wl">broadcasts: {w.broadcasts.map((b: Json) => `${b.broadcast} (${bytes(b.bytes)}, used by ${n(b.hits + 1)} task${b.hits ? "s" : ""})`).join(" · ")}</div>}
              <KV pairs={[["storage read", bytes(w.bytes.storage_read)], ["network in / out", `${bytes(w.bytes.network_in)} / ${bytes(w.bytes.network_out)}`]]} />
            </div>
          );
        })}
      </div>
      <div class="tlegend">
        {Object.entries(KINDS).map(([k, l]) => <span><i class={`sw pc-x k-${k}`} />{l}</span>)}
        <span class="note">Memory and disk strips: each item as wide as its bytes, colored by the stage holding it. Shuffle files are written to disk; a broadcast's table is held in memory on every executor that read it. The Stop button is a failure: the node is gone with its executor, its running tasks and its shuffle files; it is not replaced.</span>
      </div>
      <section class="panel stack">
        <h4>Events so far</h4>
        <ul class="clog">{state.log.map(([t, text, kind, seq]: [number, string, string, number]) => <li class={seq === state.at ? "on" : ""}><span class="faint mono">{ms(t).padStart(9)}</span> <KindChip engine={state.engine} kind={kind} /> {text}</li>)}</ul>
      </section>
    </div>
  );
}
