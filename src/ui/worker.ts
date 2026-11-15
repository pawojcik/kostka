// The engine in a Web Worker, so long loads and queries never freeze the page.
// The catalog is SQLite (sqlite-wasm) and the data files are Parquet, both in the
// origin-private file system, so tables survive a reload. Where that is not
// available (another tab holds it, or a private window), both live in memory.
// Messages: {id, name, args} -> {id, ok, value | error}; progress: {id, event}.

import { openWasmSqlite } from "../catalog/wasm-sqlite.ts";
import { Database } from "../engine/database.ts";
import { MemoryFileStore } from "../storage/file-store.ts";
import { OpfsFileStore } from "../storage/opfs-file-store.ts";
import { Session } from "./session.ts";

let session: Session | null = null;

async function boot(): Promise<string> {
  try {
    const db = await openWasmSqlite("/catalog.db", true);
    session = new Session(new Database(db, new OpfsFileStore("kostka-data")), "opfs");
    return "opfs";
  } catch (e) {
    console.warn("OPFS is not available, keeping the database in memory:", e);
    const db = await openWasmSqlite(":memory:", false);
    session = new Session(new Database(db, new MemoryFileStore()), "memory");
    return "memory";
  }
}

const ready = boot().then(
  (storage) => postMessage({ ready: true, storage }),
  (err) => postMessage({ ready: false, error: String(err) }),
);

const PROGRESS = new Set(["generate", "loadSample", "loadTpcds", "loadSsb", "loadTaxi", "finish", "run"]);
let queue: Promise<void> = Promise.resolve();

onmessage = (e: MessageEvent) => {
  const { id, name, args } = e.data as { id: number; name: string; args: unknown[] };
  queue = queue.then(async () => {
    await ready;
    try {
      if (!session) throw new Error("the engine failed to start");
      const fn = (session as unknown as Record<string, (...a: unknown[]) => unknown>)[name];
      if (typeof fn !== "function" || name.startsWith("_")) throw new Error(`unknown call ${name}`);
      const a = [...(args ?? [])];
      if (PROGRESS.has(name)) a.push((event: unknown) => postMessage({ id, event }));
      const value = await fn.apply(session, a);
      postMessage({ id, ok: true, value });
    } catch (err) {
      postMessage({ id, ok: false, error: (err as Error).message ?? String(err), errorName: (err as Error).name });
    }
  });
};
