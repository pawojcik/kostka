import { useEffect, useRef } from "preact/hooks";
import type { ComponentChildren } from "preact";
import { SqlEditor, type Catalog } from "../editor.ts";
import { CATS, ms, n, value } from "./format.ts";
import type { Json } from "./store.ts";

export function DataTable({ columns, rows, total }: { columns: { name: string; type: string }[]; rows: unknown[][]; total?: number }) {
  return (
    <div class="stack" style="gap:6px">
      <div class="scroll tall">
        <table class="data">
          <thead><tr>{columns.map((c) => <th>{c.name}<span class="ty">{c.type}</span></th>)}</tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr>{r.map((v, i) => {
                const t = columns[i]?.type ?? "string";
                return <td class={`${t === "int" || t === "float" ? "n" : ""} ${v === null ? "v-null" : t === "bool" ? "v-bool" : ""}`}>{value(v as never, t)}</td>;
              })}</tr>
            ))}
          </tbody>
        </table>
      </div>
      {total !== undefined && total > rows.length && <p class="note">First {n(rows.length)} of {n(total)} rows.</p>}
      {!rows.length && <p class="empty">No rows.</p>}
    </div>
  );
}

export function Chip({ kind = "", children }: { kind?: string; children: ComponentChildren }) {
  return <span class={`chip ${kind}`}>{children}</span>;
}

export function KV({ pairs }: { pairs: [string, ComponentChildren][] }) {
  return <dl class="kv">{pairs.map(([k, v]) => [<dt>{k}</dt>, <dd>{v}</dd>])}</dl>;
}

/** Time by category as a stacked bar; `scale` (ms) is the full width. */
export function TimeBar({ tm, scale, legend = true }: { tm: Record<string, number>; scale?: number; legend?: boolean }) {
  const total = CATS.reduce((s, [c]) => s + (tm[c] ?? 0), 0) + (tm.idle ?? 0);
  const full = scale ?? total;
  const parts = [...CATS, ["idle", "idle"] as [string, string]].filter(([c]) => (tm[c] ?? 0) > 0);
  return (
    <div class="stack" style="gap:4px">
      <div class="ttrack">
        <div class="tbar" style={`width:${full ? Math.max(1, (100 * total) / full) : 0}%`}>
          {parts.map(([c, label]) => <i class={`cat-${c}`} style={`flex-grow:${tm[c]}`} title={`${label}: ${ms(tm[c])}`} />)}
        </div>
      </div>
      {legend && (
        <div class="tlegend">
          {parts.map(([c, label]) => <span><i class={`sw cat-${c}`} />{label} <b>{ms(tm[c])}</b></span>)}
        </div>
      )}
    </div>
  );
}

export function Editor({ doc, catalog, onChange, onRun, check, readOnly, placeholder }: {
  doc: string; catalog: Catalog; onChange?: (t: string) => void; onRun?: () => void;
  check?: (t: string) => Promise<{ from: number; to: number; message: string }[]>; readOnly?: boolean; placeholder?: string;
}) {
  const host = useRef<HTMLDivElement>(null);
  const ed = useRef<SqlEditor | null>(null);
  const cbs = useRef({ onChange, onRun });
  cbs.current = { onChange, onRun };
  useEffect(() => {
    ed.current = new SqlEditor(host.current!, {
      doc, catalog, check, readOnly, placeholder,
      onChange: (t) => cbs.current.onChange?.(t), onRun: () => cbs.current.onRun?.(),
    });
    return () => ed.current?.destroy();
  }, []);
  useEffect(() => ed.current?.setCatalog(catalog), [catalog]);
  useEffect(() => {
    if (ed.current) ed.current.text = doc;
  }, [doc]);
  useEffect(() => ed.current?.setReadOnly(!!readOnly), [readOnly]);
  return <div ref={host} class="sqled" />;
}

export function ProgressBar({ done, total, status = "" }: { done: number; total: number; status?: string }) {
  return <div class={`bar big ${status}`}><i style={`width:${total ? Math.min(100, (100 * done) / total) : 0}%`} /></div>;
}

export function StateView({ state }: { state: Json }) {
  if (state === null || state === undefined) return <span class="faint">—</span>;
  if (typeof state !== "object") return <span>{String(state)}</span>;
  if (Array.isArray(state)) {
    if (!state.length) return <span class="faint">[]</span>;
    if (state.every((x) => typeof x !== "object" || x === null)) return <span>{state.map(String).join(", ")}</span>;
    return <ul>{state.map((x) => <li><StateView state={x} /></li>)}</ul>;
  }
  return <KV pairs={Object.entries(state).map(([k, v]) => [k, <StateView state={v} />])} />;
}
