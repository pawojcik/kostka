// kostka's steps and rows must not change when the code around it moves (ADR 0001):
// every SQL-vs-SQLite query on 1, 3 and 4 nodes, with the cost model, against a
// fixture recorded from main before the change (test/fixtures/golden-kostka.json).
// With 2 cores per node they are the same too: kostka uses core 0 only.
import { before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import type { Database } from "../../src/api/database.ts";
import { sqlCaseTables } from "../helpers/sql-cases.ts";
import { GOLDEN_CASES, GOLDEN_FIXTURE, goldenOf, type GoldenRun } from "../helpers/golden.ts";

const fixture = JSON.parse(readFileSync(GOLDEN_FIXTURE, "utf8")) as Record<string, GoldenRun>;

describe("kostka's steps and rows, against the fixture", () => {
  let db: Database;
  before(async () => {
    db = await sqlCaseTables();
  });
  for (const cores of [1, 2]) for (const c of GOLDEN_CASES) {
    test(`${c.key} · ${cores} core${cores === 1 ? "" : "s"}`, async () => {
      const o = { ...c.config, cluster: { ...c.config.cluster, cores, cpu: "model" as const } };
      const e = db.compile(c.sql, o).start();
      const rows = await e.run();
      assert.deepEqual(goldenOf(e.events, rows), fixture[c.key]);
    });
  }
});
