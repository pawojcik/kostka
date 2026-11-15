// SqlDb on Node's built-in node:sqlite.

import { DatabaseSync, type StatementSync } from "node:sqlite";
import type { SqlDb, SqlRow, SqlValue } from "./sqlite.ts";

export function openNodeSqlite(path = ":memory:"): SqlDb {
  const db = new DatabaseSync(path);
  const cache = new Map<string, StatementSync>();
  const prep = (sql: string) => {
    let s = cache.get(sql);
    if (!s) cache.set(sql, (s = db.prepare(sql)));
    return s;
  };
  if (path !== ":memory:") db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA foreign_keys=ON");
  return {
    exec: (sql) => db.exec(sql),
    all: (sql, params = []) => prep(sql).all(...(params as SqlValue[])) as SqlRow[],
    run: (sql, params = []) => {
      const r = prep(sql).run(...(params as SqlValue[]));
      return { lastId: Number(r.lastInsertRowid), changes: Number(r.changes) };
    },
    close: () => db.close(),
  };
}
