// The Tables tab: loading data on the left (CSV, TPC-DS, a sample, empty tables) and
// the list of tables; the selected table on the right with its rows at any version,
// its history (revert), its data files with their statistics, and a generator.

import { useState } from "preact/hooks";
import { call, callWith } from "../rpc.ts";
import { S, toast, update, type Json } from "./store.ts";
import { DataTable, ProgressBar } from "./widgets.tsx";
import { Spinner } from "./logo.tsx";
import { bytes, n, value } from "./format.ts";
import { formatTimestamp, formatValue } from "../../core/types.ts";

const FILE_ROWS = [1024, 2048, 4096, 8192, 16384, 65536];
const CSV_MODES: [string, string][] = [["create", "Create a new table"], ["replace", "Replace a table"], ["append", "Append to a table"]];
const DELIMITERS: [string, string][] = [[",", "comma"], [";", "semicolon"], ["\t", "tab"], ["|", "pipe"]];
const BENCHMARKS = {
  taxi: {
    label: "NYC taxi", call: "loadTaxi", notes: null,
    about: "Real data: 100,000 yellow taxi trips from January 2024, sampled from the NYC Taxi & Limousine Commission's trip records, and the 265 taxi zones they start and end in. A 2 MB download.",
  },
  ssb: {
    label: "SSB", call: "loadSsb", notes: { "0.01": "about 66k rows", "0.03": "about 190k rows", "0.1": "about 620k rows", "0.3": "about 1.9M rows" } as Record<string, string>,
    about: "The Star Schema Benchmark: sales (lineorder) and four dimensions, date, customer, supplier and part. Its 13 queries are short star joins, a good place to start.",
  },
  tpcds: {
    label: "TPC-DS", call: "loadTpcds", notes: { "0.01": "about 70k rows", "0.03": "about 200k rows", "0.1": "about 700k rows", "0.3": "about 2M rows" } as Record<string, string>,
    about: "The 24 tables of the TPC-DS schema: three sales channels with their returns and many dimensions. Its queries are longer and closer to real reports.",
  },
};

export async function refreshTables(tables?: Json[]): Promise<void> {
  S.tables = tables ?? (await call("tables"));
  S.catalog = { tables: await call("schema"), functions: S.info?.functions ?? [] };
  if (S.selected && !S.tables.some((t) => t.name === S.selected)) {
    S.selected = null;
    S.detail = null;
  }
  if (S.selected) await loadSelected();
  update();
}

export async function loadSelected(): Promise<void> {
  if (!S.selected) return;
  try {
    S.detail = await call("table", S.selected, S.version ?? undefined);
  } catch (e) {
    toast((e as Error).message);
  }
  update();
}

/** Runs a long table operation, showing its phases and log in the activity panel. */
async function track(title: string, name: string, ...args: unknown[]): Promise<void> {
  if (S.busy) return toast("Another operation is still running.");
  S.busy = true;
  S.activity = { title, phase: "Starting…", done: 0, total: 1, status: "running", log: [] };
  update();
  const t0 = performance.now();
  try {
    const tables = await callWith((e) => {
      const a = S.activity!;
      if (e.phase) a.phase = e.phase;
      if (e.total !== undefined) {
        a.done = e.done ?? 0;
        a.total = e.total;
      }
      if (e.log) a.log.push(e.log);
      update();
    }, name, ...args);
    S.activity!.status = "ok";
    S.activity!.phase = `Finished in ${((performance.now() - t0) / 1000).toFixed(1)} s`;
    S.activity!.done = S.activity!.total = 1;
    await refreshTables(tables);
  } catch (e) {
    S.activity!.status = "bad";
    S.activity!.phase = (e as Error).message;
    S.activity!.log.push(`Error: ${(e as Error).message}`);
  } finally {
    S.busy = false;
    update();
  }
}

function select(name: string): void {
  S.selected = name;
  S.version = null;
  S.csv = null;
  void loadSelected();
}

function Sidebar() {
  const [scale, setScale] = useState("0.01");
  const [bench, setBench] = useState<keyof typeof BENCHMARKS>("taxi");
  const b = BENCHMARKS[bench];
  const [newName, setNewName] = useState("");
  const scales: number[] = S.info?.tpcdsScales ?? [0.01];
  return (
    <div class="stack">
      <section class="panel stack">
        <h4>Load a CSV file</h4>
        <label class="btn primary file-btn">Choose a CSV file…
          <input type="file" accept=".csv,.tsv,.txt,text/csv" hidden onChange={(e) => startCsv((e.target as HTMLInputElement).files?.[0])} />
        </label>
        <p class="note">The file is read in your browser. You check the column names and types before anything is written.</p>
        <h4>Benchmark data</h4>
        <div class="row">
          <label class="field grow">Benchmark
            <select class="in" value={bench} onChange={(e) => setBench((e.target as HTMLSelectElement).value as keyof typeof BENCHMARKS)}>
              {Object.entries(BENCHMARKS).map(([k, v]) => <option value={k}>{v.label}</option>)}
            </select>
          </label>
          {b.notes && <label class="field grow">Scale factor
            <select class="in" value={scale} onChange={(e) => setScale((e.target as HTMLSelectElement).value)}>
              {scales.map((s) => <option value={String(s)}>{s} · {b.notes![String(s)] ?? ""}</option>)}
            </select>
          </label>}
        </div>
        {b.notes
          ? <button class="btn" disabled={S.busy} onClick={() => track(`Generate ${b.label} (scale ${scale})`, b.call, Number(scale))}>Generate {b.label} tables</button>
          : <button class="btn" disabled={S.busy} onClick={() => track(`Load ${b.label}`, b.call, null)}>Load {b.label} tables</button>}
        <p class="note">{b.about} {b.notes ? "Seeded random data; the" : "The"} example queries run on it. Each table becomes one new version{bench === "taxi" ? "." : "; SSB and TPC-DS both have a customer table, so loading one replaces the other's."}</p>
        <button class="link" disabled={S.busy} onClick={() => track("Load sample data", "loadSample")}>Or load a small users/orders sample</button>
        <h4>New empty table</h4>
        <div class="row">
          <input class="in grow" placeholder="name" value={newName} onInput={(e) => setNewName((e.target as HTMLInputElement).value)} />
          <button class="btn" disabled={!newName} onClick={() => { S.csv = null; S.selected = null; S.detail = { creating: newName }; setNewName(""); update(); }}>Define…</button>
        </div>
        <label class="field">Rows per data file
          <select class="in" value={String(S.settings.fileRows)} onChange={async (e) => {
            S.settings = await call("setFileRows", Number((e.target as HTMLSelectElement).value));
            update();
          }}>
            {[...new Set([...FILE_ROWS, S.settings.fileRows])].sort((a, b) => a - b).map((r) => <option value={String(r)}>{n(r)}</option>)}
          </select>
        </label>
        <p class="note">Writers cut tables into files of this many rows, one Parquet row group each. An append rewrites smaller files together with the new rows.</p>
      </section>
      <section class="panel stack">
        <h4>Tables</h4>
        {!S.tables.length && <p class="empty">No tables yet. Load a CSV file or generate data.</p>}
        <div class="tlist">
          {S.tables.map((t) => (
            <button class={`titem ${S.selected === t.name ? "on" : ""}`} onClick={() => select(t.name)}>
              <b>{t.name}</b>
              <div class="sub">{n(t.rows)} rows · {n(t.files)} files · v{t.version}</div>
            </button>
          ))}
        </div>
      </section>
    </div>
  );
}

async function startCsv(file: File | undefined): Promise<void> {
  if (!file) return;
  const text = await file.text();
  S.csv = { file: file.name, text, delimiter: ",", header: true, decimal: ".", thousands: "", dateFormat: "", timestampFormat: "", table: file.name.replace(/\.[^.]*$/, "").toLowerCase().replace(/\W+/g, "_").replace(/^(\d)/, "t_$1") || "imported", mode: "create", inferred: null, error: null };
  S.detail = null;
  S.selected = null;
  await inferCsv();
}

/** The parsing options the import panel chose. */
const csvOptions = (c: Json) => ({ delimiter: c.delimiter, header: c.header, decimal: c.decimal, thousands: c.thousands, dateFormat: c.dateFormat.trim(), timestampFormat: c.timestampFormat.trim() });
const DECIMALS: [string, string][] = [[".", "period (1.5)"], [",", "comma (1,5)"]];
const THOUSANDS: [string, string][] = [["", "none"], [",", "comma (1,234)"], [".", "period (1.234)"], [" ", "space (1 234)"], ["'", "apostrophe (1'234)"]];

async function inferCsv(): Promise<void> {
  const c = S.csv;
  try {
    c.inferred = await call("csvInfer", c.text, csvOptions(c));
    // The sample holds values of the inferred types, whatever the user picks later.
    c.inferred.types = c.inferred.columns.map((col: Json) => col.type);
    c.error = null;
  } catch (e) {
    c.inferred = null;
    c.error = (e as Error).message;
  }
  update();
}

function CsvPanel() {
  const c = S.csv;
  const types: string[] = S.info?.types ?? [];
  const importIt = async () => {
    if (S.busy) return;
    S.busy = true;
    update();
    try {
      const r = await call("csvImport", c.text, c.table, c.mode, c.inferred.columns, csvOptions(c), c.file);
      toast(`Wrote ${n(r.rows)} rows into ${c.table}.`);
      S.csv = null;
      S.busy = false;
      await refreshTables(r.tables);
      select(c.table);
    } catch (e) {
      c.error = (e as Error).message;
    } finally {
      S.busy = false;
      update();
    }
  };
  return (
    <section class="panel stack">
      <div class="row"><h3 class="grow">Import {c.file}</h3><button class="x" onClick={() => { S.csv = null; update(); }}>×</button></div>
      <div class="row">
        <label class="field">Delimiter<select class="in" value={c.delimiter} onChange={(e) => { c.delimiter = (e.target as HTMLSelectElement).value; void inferCsv(); }}>{DELIMITERS.map(([d, l]) => <option value={d}>{l}</option>)}</select></label>
        <label class="check"><input type="checkbox" checked={c.header} onChange={(e) => { c.header = (e.target as HTMLInputElement).checked; void inferCsv(); }} /> First line is a header</label>
        <label class="field">Write as<select class="in" value={c.mode} onChange={(e) => { c.mode = (e.target as HTMLSelectElement).value; update(); }}>{CSV_MODES.map(([m, l]) => <option value={m}>{l}</option>)}</select></label>
        <label class="field">Table<input class="in" value={c.table} list="table-names" onInput={(e) => { c.table = (e.target as HTMLInputElement).value; update(); }} /></label>
        <datalist id="table-names">{S.tables.map((t) => <option value={t.name} />)}</datalist>
      </div>
      <div class="row">
        <label class="field">Decimal separator<select class="in" value={c.decimal} onChange={(e) => { c.decimal = (e.target as HTMLSelectElement).value; void inferCsv(); }}>{DECIMALS.map(([d, l]) => <option value={d}>{l}</option>)}</select></label>
        <label class="field">Thousands separator<select class="in" value={c.thousands} onChange={(e) => { c.thousands = (e.target as HTMLSelectElement).value; void inferCsv(); }}>{THOUSANDS.map(([d, l]) => <option value={d}>{l}</option>)}</select></label>
        <label class="field" title="Tokens: YYYY YY, MMM MM M, DD D, HH H, hh h with A (AM/PM), mm, ss, S… (fraction), Z (offset). Empty: ISO 8601.">Date format<input class="in" value={c.dateFormat} placeholder="ISO (YYYY-MM-DD)" onChange={(e) => { c.dateFormat = (e.target as HTMLInputElement).value; void inferCsv(); }} /></label>
        <label class="field" title="Tokens: YYYY YY, MMM MM M, DD D, HH H, hh h with A (AM/PM), mm, ss, S… (fraction), Z (offset). Empty: ISO 8601.">Timestamp format<input class="in" value={c.timestampFormat} placeholder="ISO (YYYY-MM-DD HH:mm:ss)" onChange={(e) => { c.timestampFormat = (e.target as HTMLInputElement).value; void inferCsv(); }} /></label>
      </div>
      <p class="note">Formats use YYYY, MM, DD, HH, mm, ss (and M, D, MMM, YY, hh with A for AM/PM, S for fractions, Z for an offset), e.g. DD/MM/YYYY or M/D/YYYY h:mm A.</p>
      {c.error && <div class="error">{c.error}</div>}
      {c.inferred && (
        <>
          <p class="note">{n(c.inferred.rows)} rows, {bytes(c.inferred.bytes)}. Types are inferred from every value; change them or leave columns out.</p>
          <div class="scroll">
            <table class="data csv-schema">
              <thead><tr><th>import</th><th>CSV column</th><th>name</th><th>type</th><th>sample</th></tr></thead>
              <tbody>
                {c.inferred.columns.map((col: Json, i: number) => (
                  <tr class={col.include ? "" : "off"}>
                    <td><input type="checkbox" checked={col.include} onChange={(e) => { col.include = (e.target as HTMLInputElement).checked; update(); }} /></td>
                    <td>{col.source}</td>
                    <td><input class="in" value={col.name} onInput={(e) => { col.name = (e.target as HTMLInputElement).value; }} /></td>
                    <td><select class="in" value={col.type} onChange={(e) => { col.type = (e.target as HTMLSelectElement).value; update(); }}>{types.map((t) => <option>{t}</option>)}</select></td>
                    <td class="sample">{c.inferred.sample.slice(0, 5).map((r: Json[]) => (r[i] === null ? "null" : formatValue(r[i], c.inferred.types[i]))).join(", ")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div class="row"><button class="btn primary" disabled={S.busy || !c.table} onClick={importIt}>Import</button></div>
        </>
      )}
    </section>
  );
}

function CreatePanel({ name }: { name: string }) {
  const [cols, setCols] = useState<[string, string][]>([["id", "int"], ["name", "string"]]);
  const [rows, setRows] = useState(1000);
  const types: string[] = S.info?.types ?? [];
  const set = (i: number, j: 0 | 1, v: string) => setCols(cols.map((c, k) => (k === i ? (j ? [c[0], v] : [v, c[1]]) as [string, string] : c)));
  return (
    <section class="panel stack">
      <div class="row"><h3 class="grow">New table {name}</h3><button class="x" onClick={() => { S.detail = null; update(); }}>×</button></div>
      {cols.map((c, i) => (
        <div class="row">
          <input class="in" value={c[0]} onInput={(e) => set(i, 0, (e.target as HTMLInputElement).value)} />
          <select class="in" value={c[1]} onChange={(e) => set(i, 1, (e.target as HTMLSelectElement).value)}>{types.map((t) => <option>{t}</option>)}</select>
          <button class="x" onClick={() => setCols(cols.filter((_, k) => k !== i))}>×</button>
        </div>
      ))}
      <div class="row">
        <button class="btn small" onClick={() => setCols([...cols, [`c${cols.length + 1}`, "int"]])}>Add a column</button>
        <label class="field">Rows to generate<input class="in num" type="number" min="0" value={rows} onInput={(e) => setRows(Number((e.target as HTMLInputElement).value))} /></label>
        <button class="btn primary" disabled={S.busy} onClick={async () => {
          await track(`Create ${name}`, "generate", name, rows, { columns: cols, sortBy: cols[0]?.[0] });
          select(name);
        }}>Create</button>
      </div>
      <p class="note">Values come from each column's default generator, chosen from its name and type (an <code>id</code> is a sequence, a <code>name</code> or <code>city</code> picks from a list). The table can be refilled with other generators afterwards.</p>
    </section>
  );
}

function GeneratePanel({ d }: { d: Json }) {
  const [rows, setRows] = useState(5000);
  const [mode, setMode] = useState("append");
  const [sortBy, setSortBy] = useState("");
  const [gens, setGens] = useState<Record<string, Json> | null>(null);
  const kinds: Record<string, string[]> = S.info?.generatorKinds ?? {};
  const load = async () => setGens(await call("defaultGenerators", d.name));
  return (
    <details class="panel" onToggle={(e) => (e.target as HTMLDetailsElement).open && !gens && load()}>
      <summary>Generate rows</summary>
      {gens && (
        <div class="stack" style="margin-top:10px">
          <div class="gens">
            {d.columns.map((c: Json) => {
              const g = gens[c.name] ?? { kind: kinds[c.type]?.[0] };
              const setG = (patch: Json) => setGens({ ...gens, [c.name]: { ...g, ...patch } });
              return (
                <div class="gen">
                  <div class="head"><b>{c.name}</b><span class="muted">{c.type}</span></div>
                  <select class="in" value={g.kind} onChange={(e) => setG({ kind: (e.target as HTMLSelectElement).value })}>{(kinds[c.type] ?? []).map((k) => <option>{k}</option>)}</select>
                  {Object.entries(g).filter(([k]) => k !== "kind").map(([k, v]) => (
                    <label class="field">{k}<input class="in" value={Array.isArray(v) ? v.join(", ") : String(v)} onChange={(e) => {
                      const t = (e.target as HTMLInputElement).value;
                      setG({ [k]: Array.isArray(v) ? t.split(",").map((x) => x.trim()).map((x) => (c.type === "int" || c.type === "float" ? Number(x) : x)) : typeof v === "number" ? Number(t) : t });
                    }} /></label>
                  ))}
                </div>
              );
            })}
          </div>
          <div class="row">
            <label class="field">Rows<input class="in num" type="number" min="0" value={rows} onInput={(e) => setRows(Number((e.target as HTMLInputElement).value))} /></label>
            <label class="field">Mode<select class="in" value={mode} onChange={(e) => setMode((e.target as HTMLSelectElement).value)}><option value="append">append</option><option value="replace">replace</option></select></label>
            <label class="field">Sort by<select class="in" value={sortBy} onChange={(e) => setSortBy((e.target as HTMLSelectElement).value)}><option value="">(no sorting)</option>{d.columns.map((c: Json) => <option>{c.name}</option>)}</select></label>
            <button class="btn primary" disabled={S.busy} onClick={() => track(`Generate ${n(rows)} rows into ${d.name}`, "generate", d.name, rows, { generators: gens, mode, sortBy: sortBy || null, seed: Date.now() % 1e9 })}>Generate</button>
          </div>
          <p class="note">Sorting by a column makes each data file cover a narrow range of it, so file pruning can skip files.</p>
        </div>
      )}
    </details>
  );
}

function TableDetail({ d }: { d: Json }) {
  return (
    <div class="stack">
      <section class="panel stack">
        <div class="row">
          <h3 class="grow">{d.name} <span class="muted">v{d.version}{d.version !== d.current ? ` (current: v${d.current})` : ""}</span></h3>
          <span class="note">{n(d.total)} rows · {n(d.files.length)} files · {bytes(d.files.reduce((s: number, f: Json) => s + f.bytes, 0))}</span>
          <button class="btn danger" disabled={S.busy} onClick={async () => {
            if (!confirm(`Drop table ${d.name}? Its data files are deleted.`)) return;
            await refreshTables(await call("dropTable", d.name));
          }}>Drop table</button>
        </div>
        <h4>Data <span class="muted">first {n(d.rows.length)} rows</span></h4>
        <DataTable columns={d.columns} rows={d.rows} total={d.total} />
      </section>
      <GeneratePanel d={d} />
      <section class="panel stack">
        <h4>History</h4>
        <div class="scroll short">
          <table class="data">
            <thead><tr><th>version</th><th>committed (UTC)</th><th>operation</th><th>rows</th><th>files</th><th>detail</th><th>schema</th><th /></tr></thead>
            <tbody>
              {[...d.history].reverse().map((h: Json) => (
                <tr>
                  <td><button class="link" onClick={() => { S.version = h.version; void loadSelected(); }}>{h.version === d.version ? <b>v{h.version}</b> : `v${h.version}`}</button></td>
                  <td>{formatTimestamp(h.createdUs).slice(0, 19)}</td><td>{h.operation}</td><td class="n">{n(h.rows)}</td><td class="n">{n(h.files)}</td>
                  <td>{h.detail}</td><td title={h.columns.join(", ")}>{h.columns.length} columns</td>
                  <td>{h.version !== d.current && <button class="btn small" onClick={async () => { await refreshTables(await call("revert", d.name, h.version)); S.version = null; await loadSelected(); }}>Revert to</button>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p class="note">Every load is one new version; nothing is deleted. Click a version to see its rows. A revert adds a version with the files of an old one. Queries read a version with <code>FOR VERSION AS OF n</code> or <code>FOR TIMESTAMP AS OF '…'</code>.</p>
      </section>
      <section class="panel stack">
        <h4>Data files of v{d.version}</h4>
        <div class="scroll short">
          <table class="data">
            <thead><tr><th>#</th><th>file</th><th>rows</th><th>size</th>{d.columns.map((c: Json) => <th>{c.name}<span class="ty">min … max · nulls · distinct</span></th>)}</tr></thead>
            <tbody>
              {d.files.map((f: Json) => (
                <tr>
                  <td class="n">{f.position}</td><td>{f.path}</td><td class="n">{n(f.rows)}</td><td class="n">{bytes(f.bytes)}</td>
                  {d.columns.map((c: Json) => {
                    const s = f.stats.find((x: Json) => x.column === c.name);
                    return <td title={s?.bloom ? "has a bloom filter" : ""}>{s ? `${s.min === null ? "—" : value(s.min, c.type)} … ${s.max === null ? "—" : value(s.max, c.type)} · ${n(s.nulls)} · ${n(s.distinct)}` : ""}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p class="note">Each file is one Parquet row group. The optimizer prunes files by these statistics (and bloom filters on string columns) and estimates join sizes from per-file distinct-count sketches.</p>
      </section>
    </div>
  );
}

function ActivityPanel() {
  const a = S.activity!;
  return (
    <section class="panel stack">
      <div class="row"><h3 class="grow">Activity</h3>{!S.busy && <button class="link" onClick={() => { S.activity = null; update(); }}>Clear</button>}</div>
      <div class="row">{a.status === "running" && <Spinner size={22} />}<b class="grow">{a.title}</b><span class={`chip ${a.status === "ok" ? "ok" : a.status === "bad" ? "bad" : "run"}`}>{a.status === "running" ? "running" : a.status === "ok" ? "done" : "failed"}</span></div>
      <ProgressBar done={a.done} total={a.total} status={a.status === "ok" ? "ok" : a.status === "bad" ? "bad" : ""} />
      <div class="note">{a.phase}{a.status === "running" && a.total > 1 ? ` · ${n(a.done)} / ${n(a.total)}` : ""}</div>
      {a.log.length > 0 && <div class="log">{a.log.map((l) => <div>{l}</div>)}</div>}
    </section>
  );
}

export function TablesTab() {
  return (
    <div class="side">
      <Sidebar />
      <div class="stack">
        {S.activity && <ActivityPanel />}
        {S.csv && <CsvPanel />}
        {S.detail?.creating && <CreatePanel name={S.detail.creating} />}
        {S.detail && !S.detail.creating && <TableDetail d={S.detail} />}
        {!S.csv && !S.detail && !S.activity && (
          <section class="panel"><p class="note">{S.tables.length ? "Pick a table on the left, or open a query tab." : "Load a CSV file or generate data to get started."}</p></section>
        )}
        {S.storage && <p class="note faint">{S.storage === "opfs" ? "Tables are stored in this browser (origin-private file system) and survive a reload." : "Tables are kept in memory: this browser could not open its private file system (another tab may hold it), so a reload starts empty."}</p>}
      </div>
    </div>
  );
}

