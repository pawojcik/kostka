// The engine against SQLite on small random tables with nulls and duplicate keys:
// every query, on 1 to 4 workers, with every join strategy and small batches and
// splits, must return the same rows (in order when it sorts by a unique key, else as
// a multiset).
import { before, describe, test } from "node:test";
import type { DatabaseSync } from "node:sqlite";
import type { Database } from "../../src/api/database.ts";
import type { Value } from "../../src/core/types.ts";
import { assertRows, assertSameRows } from "../helpers/compare.ts";
import { sqliteCopy } from "../helpers/db.ts";
import { CONFIGS, SORTED, UNSORTED, sqlCaseTables } from "../helpers/sql-cases.ts";

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
        for (const o of CONFIGS) check((await db.query(sql, o)).rows, want, JSON.stringify(o));
      });
    }
  }
});
