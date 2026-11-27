// A Database in memory with a few small tables, and a SQLite copy of them.
import { DatabaseSync } from "node:sqlite";
import { openDatabase } from "../../src/node.ts";
import type { Database } from "../../src/api/database.ts";
import type { Value } from "../../src/core/types.ts";

export async function tables(defs: Record<string, { columns: [string, string][]; rows: Value[][] }>, fileRows = 4): Promise<Database> {
  const db = openDatabase();
  db.fileRows = fileRows;
  for (const [name, t] of Object.entries(defs)) {
    await db.createTable(name, t.columns);
    if (t.rows.length) await db.append(name, t.rows);
  }
  return db;
}

export async function sqliteCopy(db: Database): Promise<DatabaseSync> {
  const s = new DatabaseSync(":memory:");
  for (const t of db.listTables()) {
    const cols = db.describe(t).columns;
    s.exec(`create table ${t} (${cols.map((c) => c.name).join(", ")})`);
    const ins = s.prepare(`insert into ${t} values (${cols.map(() => "?").join(", ")})`);
    for (const r of (await db.query(`select * from ${t}`)).rows) ins.run(...(r.map((v) => (typeof v === "boolean" ? Number(v) : v)) as never[]));
  }
  return s;
}

export async function rows(db: Database, sql: string, o = {}): Promise<Value[][]> {
  return (await db.query(sql, o)).rows;
}
