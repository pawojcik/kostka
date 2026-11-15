import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { openDatabase } from "../../src/engine/node.ts";
import type { Database } from "../../src/engine/database.ts";
import { QUERIES } from "../../src/loaders/taxi/queries.ts";
import { FILES, SCHEMA, TABLES } from "../../src/loaders/taxi/load.ts";
import { assertRows } from "../helpers/compare.ts";

const files = Object.fromEntries(Object.entries(FILES).map(([t, p]) => [t, new Uint8Array(readFileSync(new URL(`../../docs/${p}`, import.meta.url)))]));

describe("NYC taxi", () => {
  let db: Database;
  let counts: Record<string, number>;
  const sqlite = new DatabaseSync(":memory:");

  before(async () => {
    db = openDatabase();
    counts = await db.loadTaxi(files);
    for (const t of TABLES) {
      const names = SCHEMA[t]!.map((c) => c.name);
      sqlite.exec(`create table ${t} (${names.join(", ")})`);
      const ins = sqlite.prepare(`insert into ${t} values (${names.map(() => "?").join(", ")})`);
      const { rows } = await db.query(`select * from ${t}`);
      sqlite.exec("begin");
      for (const r of rows) ins.run(...(r as never[]));
      sqlite.exec("commit");
    }
  });

  test("the sample's tables", async () => {
    assert.deepEqual(counts, { zones: 265, trips: 100_000 });
    for (const t of TABLES) assert.deepEqual(db.describe(t).columns.map((c) => c.name), SCHEMA[t]!.map((c) => c.name));
    assert.deepEqual((await db.query("select borough, zone from zones where zone_id = 132")).rows, [["Queens", "JFK Airport"]]);
    const { rows } = await db.query("select min(pickup_time) >= timestamp '2024-01-01 00:00:00', max(pickup_time) < timestamp '2024-02-01 00:00:00', min(total) >= 0 from trips");
    assert.deepEqual(rows, [[true, true, true]]);
  });

  test("trips are in pickup order, so files cover narrow time ranges", () => {
    const f = db.files("trips");
    assert.ok(f.length > 1);
    for (let i = 1; i < f.length; i++) assert.ok((f[i - 1]!.stats.get("pickup_time")!.max as number) <= (f[i]!.stats.get("pickup_time")!.min as number));
  });

  test("time functions", async () => {
    const { rows } = await db.query("select hour(t), minute(t), day_of_week(t), minutes_between(t, timestamp '2024-01-21 10:00:00') from (select timestamp '2024-01-21 08:45:30' t from zones where zone_id = 1) x");
    assert.deepEqual(rows, [[8, 45, 7, 74.5]]);
  });

  for (const q of QUERIES) {
    test(`${q.id} matches SQLite`, async () => {
      const want = sqlite.prepare(q.reference ?? q.sql).all().map((r) => Object.values(r as object));
      assert.ok(want.length > 0, "the query selects no rows");
      for (const [workers, joinStrategy] of [[1, "auto"], [4, "auto"], [8, "shuffle"]] as const) {
        const { rows } = await db.query(q.sql, { cluster: { workers, batchRows: 1024, splitRows: 4096 }, planner: { joinStrategy } });
        assertRows(rows, want, `${q.id} on ${workers} worker(s), ${joinStrategy}`);
      }
    });
  }
});
