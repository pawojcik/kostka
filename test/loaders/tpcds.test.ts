import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openDatabase } from "../../src/engine/node.ts";
import type { Database } from "../../src/engine/database.ts";
import { QUERIES } from "../../src/loaders/tpcds/queries.ts";
import { SCHEMA, TABLES, dateSk, rowCounts } from "../../src/loaders/tpcds/generate.ts";
import { daysOf } from "../../src/core/types.ts";
import { assertRows } from "../helpers/compare.ts";

const SCALE = 0.01;

describe("TPC-DS", () => {
  let db: Database;
  let counts: Record<string, number>;
  const progress: [string, number, number][] = [];
  const sqlite = new DatabaseSync(":memory:");

  before(async () => {
    db = openDatabase();
    counts = await db.loadTpcds(SCALE, { report: (t, d, n) => progress.push([t, d, n]) });
    for (const t of TABLES) {
      const names = SCHEMA[t]!.map((c) => c.name);
      sqlite.exec(`create table ${t} (${names.join(", ")})`);
      const ins = sqlite.prepare(`insert into ${t} values (${names.map(() => "?").join(", ")})`);
      const { rows } = await db.query(`select * from ${t}`);
      sqlite.exec("begin");
      for (const r of rows) ins.run(...(r.map((v) => (typeof v === "boolean" ? Number(v) : v)) as never[]));
      sqlite.exec("commit");
    }
  });

  test("the schema is the standard one", () => {
    assert.equal(TABLES.length, 24);
    assert.equal(SCHEMA.store_sales!.length, 23);
    assert.equal(SCHEMA.date_dim!.length, 28);
    for (const t of TABLES) assert.deepEqual(db.describe(t).columns.map((c) => c.name), SCHEMA[t]!.map((c) => c.name));
  });

  test("row counts", () => {
    const expected = rowCounts(SCALE);
    assert.equal(counts.date_dim, 73_049);
    assert.equal(counts.time_dim, 86_400);
    for (const t of TABLES) {
      const files = db.files(t);
      assert.equal(files.reduce((s, f) => s + f.rowCount, 0), counts[t], t);
      if (!t.endsWith("_sales") && !t.endsWith("_returns")) assert.equal(counts[t], expected[t], t);
      else assert.ok(Math.abs(counts[t]! - expected[t]!) < expected[t]! * 0.2, `${t}: ${counts[t]} vs ${expected[t]}`);
    }
  });

  test("keys and dates", async () => {
    const { rows } = await db.query("select d_date_sk, d_date, d_year, d_moy, d_dom, d_day_name from date_dim where d_date_sk = 2451545");
    assert.deepEqual(rows, [[2451545, daysOf(2000, 1, 1), 2000, 1, 1, "Saturday"]]);
    assert.equal(dateSk(daysOf(2000, 1, 1)), 2451545);
  });

  test("sales are in date order", async () => {
    const files = db.files("store_sales");
    assert.ok(files.length > 1);
    for (let i = 1; i < files.length; i++) {
      const prev = files[i - 1]!.stats.get("ss_sold_date_sk")!, cur = files[i]!.stats.get("ss_sold_date_sk")!;
      assert.ok((prev.max as number) <= (cur.min as number) + 0, `file ${i}`);
    }
  });

  test("progress reaches every table", () => {
    for (const t of TABLES) assert.ok(progress.some(([x, d, n]) => x === t && d === n && n === counts[t]), t);
  });

  test("generation is deterministic", async () => {
    const other = openDatabase();
    await other.loadTpcds(SCALE);
    for (const t of ["item", "customer", "web_returns"]) {
      assert.deepEqual((await other.query(`select * from ${t} order by 1, 2, 3`)).rows, (await db.query(`select * from ${t} order by 1, 2, 3`)).rows, t);
    }
  });

  for (const q of QUERIES) {
    test(`${q.id} matches SQLite`, async () => {
      const want = sqlite.prepare(q.reference ?? q.sql).all().map((r) => Object.values(r as object));
      assert.ok(want.length > 0, "the parameters select no rows");
      for (const [workers, joinStrategy] of [[1, "auto"], [4, "auto"], [8, "shuffle"]] as const) {
        const { rows } = await db.query(q.sql, { cluster: { workers, batchRows: 1024, splitRows: 4096 }, planner: { joinStrategy } });
        assertRows(rows, want, `${q.id} on ${workers} worker(s), ${joinStrategy}`);
      }
    });
  }
});
