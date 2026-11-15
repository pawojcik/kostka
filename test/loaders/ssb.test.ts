import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { openDatabase } from "../../src/engine/node.ts";
import type { Database } from "../../src/engine/database.ts";
import { QUERIES } from "../../src/loaders/ssb/queries.ts";
import { SCHEMA, TABLES, rowCounts } from "../../src/loaders/ssb/generate.ts";
import { assertRows } from "../helpers/compare.ts";

const SCALE = 0.01;
/** Q3.4 is the most selective query: at this scale it finds no rows. */
const MAY_BE_EMPTY = new Set(["ssb3.4"]);

describe("SSB", () => {
  let db: Database;
  let counts: Record<string, number>;
  const progress: [string, number, number][] = [];
  const sqlite = new DatabaseSync(":memory:");

  before(async () => {
    db = openDatabase();
    counts = await db.loadSsb(SCALE, { report: (t, d, n) => progress.push([t, d, n]) });
    for (const t of TABLES) {
      const names = SCHEMA[t]!.map((c) => c.name);
      sqlite.exec(`create table "${t}" (${names.join(", ")})`);
      const ins = sqlite.prepare(`insert into "${t}" values (${names.map(() => "?").join(", ")})`);
      const { rows } = await db.query(`select * from ${t}`);
      sqlite.exec("begin");
      for (const r of rows) ins.run(...(r as never[]));
      sqlite.exec("commit");
    }
  });

  test("the schema is the standard one", () => {
    assert.deepEqual(TABLES, ["date", "customer", "supplier", "part", "lineorder"]);
    assert.equal(SCHEMA.lineorder!.length, 17);
    assert.equal(SCHEMA.date!.length, 17);
    for (const t of TABLES) assert.deepEqual(db.describe(t).columns.map((c) => c.name), SCHEMA[t]!.map((c) => c.name));
  });

  test("row counts", () => {
    assert.deepEqual(counts, rowCounts(SCALE));
    assert.equal(counts.date, 2557);
    assert.equal(counts.lineorder, 60_000);
    for (const t of TABLES) assert.equal(db.files(t).reduce((s, f) => s + f.rowCount, 0), counts[t], t);
  });

  test("dates, cities and brands", async () => {
    assert.deepEqual((await db.query("select d_date, d_dayofweek, d_yearmonth, d_weeknuminyear, d_sellingseason from date where d_datekey = 19940206")).rows,
      [["February 6, 1994", "Sunday", "Feb1994", 6, "Winter"]]);
    const distinct = async (c: string, t: string) => (await db.query(`select count(*) from (select distinct ${c} from ${t}) x`)).rows[0]![0];
    assert.deepEqual([await distinct("c_nation", "customer"), await distinct("c_region", "customer")], [25, 5]);
    assert.ok((await distinct("c_city", "customer") as number) > 200);
    assert.equal(await distinct("p_brand1", "part"), 1000);
    assert.deepEqual((await db.query("select min(p_brand1), max(p_category) from part")).rows, [["MFGR#111", "MFGR#55"]]);
  });

  test("lineorder is in date order", () => {
    const files = db.files("lineorder");
    assert.ok(files.length > 1);
    for (let i = 1; i < files.length; i++) {
      assert.ok((files[i - 1]!.stats.get("lo_orderdate")!.max as number) <= (files[i]!.stats.get("lo_orderdate")!.min as number), `file ${i}`);
    }
  });

  test("progress reaches every table", () => {
    for (const t of TABLES) assert.ok(progress.some(([x, d, n]) => x === t && d === n && n === counts[t]), t);
  });

  for (const q of QUERIES) {
    test(`${q.id} matches SQLite`, async () => {
      const want = sqlite.prepare(q.sql.replace(/\bdate\b(?=,|\s+where|\s*$)/gm, '"date"')).all().map((r) => Object.values(r as object));
      if (!MAY_BE_EMPTY.has(q.id)) assert.ok(want.length > 0, "the parameters select no rows");
      for (const [workers, joinStrategy] of [[1, "auto"], [4, "auto"], [8, "shuffle"]] as const) {
        const { rows } = await db.query(q.sql, { cluster: { workers, batchRows: 1024, splitRows: 4096 }, planner: { joinStrategy } });
        assertRows(rows, want, `${q.id} on ${workers} worker(s), ${joinStrategy}`);
      }
    });
  }
});
