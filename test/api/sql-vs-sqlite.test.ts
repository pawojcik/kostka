// The engines against SQLite on small random tables with nulls and duplicate keys:
// every query, on kostka with 1 to 4 workers, every join strategy and small batches
// and splits, and on the Spark-style engine with 1, 3 and 4 nodes, 1 and 2 cores, and
// broadcasts off or always (ADR 0002, criterion 1), must return the same rows (in
// order when it sorts by a unique key, else as a multiset).
import { before, describe, test } from "node:test";
import type { DatabaseSync } from "node:sqlite";
import type { Database } from "../../src/api/database.ts";
import type { Value } from "../../src/core/types.ts";
import { assertRows, assertSameRows } from "../helpers/compare.ts";
import { sqliteCopy } from "../helpers/db.ts";
import { CONFIGS, SORTED, SPARK_CONFIGS, UNSORTED, sqlCaseTables } from "../helpers/sql-cases.ts";

describe("SQL against SQLite", () => {
  let db: Database;
  let sqlite: DatabaseSync;
  before(async () => {
    db = await sqlCaseTables();
    sqlite = await sqliteCopy(db);
  });
  for (const [list, check] of [[SORTED, assertRows], [UNSORTED, assertSameRows]] as const) {
    for (const sql of list) {
      test(sql, async () => {
        const want = sqlite.prepare(sql).all().map((r) => Object.values(r as object)) as Value[][];
        for (const o of [...CONFIGS, ...SPARK_CONFIGS]) check((await db.query(sql, o)).rows, want, JSON.stringify(o));
      });
    }
  }
});
