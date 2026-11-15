// The workbench page: a header with the engine's status and the Tutorial button, tabs (Tables, one per open
// query, and "+ New query"), and a toast. The engine runs in a worker (see rpc.ts).

import { render } from "preact";
import { call, onActivity, ready } from "./rpc.ts";
import { S, setRenderer, toast, update } from "./app/store.ts";
import { TablesTab, refreshTables } from "./app/tables.tsx";
import { Logo, Spinner } from "./app/logo.tsx";
import { NewQueryTab, QueryTab, closeQuery } from "./app/query.tsx";
import { Tutorial, TutorialButton } from "./app/tutorial.tsx";

function App() {
  const q = S.queries.find((x) => x.key === S.tab);
  const booting = !S.ready && !S.failed;
  const wait = booting ? S.status : S.waiting;
  return (
    <>
      <header class="top">
        <div class="brand"><Logo solving={wait !== null} label={wait ?? undefined} />kostka <span class="muted">workbench</span></div>
        <div class="status" role="status">{wait ?? S.status}{S.ready && !wait && !S.tables.length ? " · load a dataset or start a tutorial" : ""}</div>
        <TutorialButton />
      </header>
      <nav class="tabs" aria-label="Workbench tabs">
        <button class={`tab ${S.tab === "tables" ? "on" : ""}`} onClick={() => { S.tab = "tables"; update(); }}>Tables</button>
        {S.queries.map((x) => (
          <button class={`tab ${S.tab === x.key ? "on" : ""}`} onClick={() => { S.tab = x.key; update(); }}>
            {x.name}
            <span class="x" title="Close this query" onClick={(e) => { e.stopPropagation(); closeQuery(x); }}>×</span>
          </button>
        ))}
        <button class={`tab ${S.tab === "new" ? "on" : ""}`} disabled={!S.ready} onClick={() => { S.tab = "new"; update(); }}>+ New query</button>
      </nav>
      <main class="main">
        {booting ? <div class="boot"><Spinner size={72}>{S.status}</Spinner></div> : !S.ready ? <div class="error">{S.status}</div> : S.tab === "tables" ? <TablesTab /> : S.tab === "new" ? <NewQueryTab /> : q ? <QueryTab q={q} /> : <TablesTab />}
      </main>
      <Tutorial />
      {S.toast && <div class="toast" role="alert" onClick={() => { S.toast = null; update(); }}>{S.toast}</div>}
    </>
  );
}

// What a call is shown as while the page waits for it.
const WAITS: Record<string, string> = {
  compile: "Compiling…", step: "Running a step…", wave: "Running a wave…", finish: "Running…", run: "Running the query…",
  state: "Loading that step…", table: "Reading the table…", tables: "Refreshing the tables…", schema: "Refreshing the tables…",
  csvInfer: "Reading the CSV file…", csvImport: "Importing the CSV file…", generate: "Generating rows…",
  loadSample: "Loading the sample…", loadTpcds: "Generating TPC-DS…", loadSsb: "Generating SSB…", loadTaxi: "Loading NYC taxi…", createTable: "Creating the table…",
  dropTable: "Dropping the table…", revert: "Reverting…", info: "Starting…", settings: "Starting…",
};
// A call shows as a wait only once it has taken a moment, so quick ones don't flicker.
let waitTimer: ReturnType<typeof setTimeout> | undefined;
onActivity((names) => {
  clearTimeout(waitTimer);
  if (!names.length) {
    if (S.waiting !== null) { S.waiting = null; update(); }
    return;
  }
  const text = WAITS[names[names.length - 1]!] ?? "Working…";
  if (S.waiting !== null) { S.waiting = text; update(); return; }
  waitTimer = setTimeout(() => { S.waiting = text; update(); }, 150);
});

const root = document.getElementById("app")!;
setRenderer(() => render(<App />, root));
update();

async function boot(): Promise<void> {
  const r = await ready;
  if (!r.ok) {
    S.status = `The engine failed to start: ${r.error}`;
    S.failed = true;
    return update();
  }
  S.storage = r.storage ?? "";
  S.info = await call("info");
  S.settings = await call("settings");
  await refreshTables();
  S.ready = true;
  S.status = S.storage === "opfs" ? "Ready · tables are kept in this browser" : "Ready · in memory (tables are lost on reload)";
  update();
}

boot().catch((e) => {
  S.status = `The engine failed to start: ${(e as Error).message}`;
  S.failed = true;
  toast(S.status);
});
