// SqlDb on sqlite-wasm in a browser worker, stored in the origin-private file system
// through the SAH-pool VFS (needs no COOP/COEP headers).

import sqlite3InitModule from "@sqlite.org/sqlite-wasm";
import type { SqlDb, SqlRow } from "./sqlite.ts";

export async function openWasmSqlite(file = "/catalog.db", persistent = true): Promise<SqlDb> {
  const sqlite3 = await sqlite3InitModule();
  let db: InstanceType<typeof sqlite3.oo1.DB>;
  if (persistent) {
    const pool = await sqlite3.installOpfsSAHPoolVfs({ name: "kostka-pool" });
    db = new pool.OpfsSAHPoolDb(file);
  } else {
    db = new sqlite3.oo1.DB(":memory:");
  }
  db.exec("PRAGMA foreign_keys=ON");
  return {
    exec: (sql) => void db.exec(sql),
    all: (sql, params = []) => db.exec({ sql, bind: params.length ? (params as never) : undefined, rowMode: "object", returnValue: "resultRows" }) as unknown as SqlRow[],
    run: (sql, params = []) => {
      db.exec({ sql, bind: params.length ? (params as never) : undefined });
      return { lastId: Number(db.selectValue("SELECT last_insert_rowid()")), changes: db.changes() };
    },
    close: () => db.close(),
  };
}
