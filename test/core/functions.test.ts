import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { AnalysisError, ExecutionError } from "../../src/core/errors.ts";
import { daysOf, parseTimestamp } from "../../src/core/types.ts";
import type { Database } from "../../src/engine/database.ts";
import { rows, tables } from "../helpers/db.ts";

describe("scalar functions and operators", () => {
  let db: Database;
  before(async () => {
    db = await tables({
      t: { columns: [["i", "int"], ["f", "float"], ["s", "string"], ["b", "bool"], ["d", "date"], ["ts", "timestamp"]], rows: [[7, 2.5, "Hello", true, daysOf(2024, 2, 28), parseTimestamp("2024-02-28 13:45:10")!]] },
      n: { columns: [["i", "int"], ["s", "string"], ["b", "bool"]], rows: [[null, null, null]] },
    });
  });
  const one = async (expr: string, table = "t") => (await rows(db, `select ${expr} from ${table}`))[0]![0];

  test("arithmetic keeps ints exact and divides as float", async () => {
    assert.equal(await one("i + 1"), 8);
    assert.equal(await one("i * i - 1"), 48);
    assert.equal(await one("i / 2"), 3.5);
    assert.equal(await one("i % 3"), 1);
    assert.equal(await one("-i % 3"), -1);
    assert.equal(await one("i + f"), 9.5);
    assert.equal(await one("-f"), -2.5);
  });

  test("int overflow is an error, not a lost digit", async () => {
    await assert.rejects(one("9007199254740991 + i"), ExecutionError);
    await assert.rejects(one("4503599627370496 * i"), ExecutionError);
  });

  test("three-valued logic", async () => {
    assert.equal(await one("b and null", "t"), null);
    assert.equal(await one("b or null"), true);
    assert.equal(await one("not b"), false);
    assert.equal(await one("b and false", "n"), false);
    assert.equal(await one("b or true", "n"), true);
    assert.equal(await one("i = 1", "n"), null);
    assert.equal(await one("i is null", "n"), true);
    assert.equal(await one("i in (1, 2)", "n"), null);
    assert.equal(await one("i in (1, 7)"), true);
    assert.equal(await one("i in (1, null)"), null);
    assert.equal(await one("i not in (1, 2)"), true);
    assert.equal(await one("i between 7 and 8"), true);
  });

  test("strings", async () => {
    assert.equal(await one("upper(s) || lower(s)"), "HELLOhello");
    assert.equal(await one("length(s)"), 5);
    assert.equal(await one("substring(s, 2, 3)"), "ell");
    assert.equal(await one("substr(s, 2)"), "ello");
    assert.equal(await one("s like 'H_l%'"), true);
    assert.equal(await one("s like 'h%'"), false);
    assert.equal(await one("replace(s, 'l', 'L')"), "HeLLo");
    assert.equal(await one("trim('  x ')"), "x");
    assert.equal(await one("starts_with(s, 'He') and ends_with(s, 'lo') and contains(s, 'll')"), true);
    assert.equal(await one("s || null"), null);
  });

  test("dates and timestamps", async () => {
    assert.equal(await one("year(d) * 10000 + month(d) * 100 + day(d)"), 20240228);
    assert.equal(await one("date_add(d, 2)"), daysOf(2024, 3, 1));
    assert.equal(await one("date_diff(to_date('2024-01-01'), d)"), 58);
    assert.equal(await one("d = '2024-02-28'"), true);
    assert.equal(await one("ts > d"), true);
    assert.equal(await one("cast(ts as date) = d"), true);
    assert.equal(await one("date_trunc('hour', ts)"), parseTimestamp("2024-02-28 13:00:00"));
    await assert.rejects(one("cast('2024-02-30' as date)"), AnalysisError); // folded at analysis
    await assert.rejects(one("cast(s as date)"), ExecutionError);
  });

  test("case, coalesce, nullif, math", async () => {
    assert.equal(await one("case when i > 5 then 'big' when i > 2 then 'mid' else 'small' end"), "big");
    assert.equal(await one("case i when 7 then 1 else 0 end"), 1);
    assert.equal(await one("case when i > 100 then 1 end"), null);
    assert.equal(await one("coalesce(i, 0)", "n"), 0);
    assert.equal(await one("nullif(i, 7)"), null);
    assert.equal(await one("abs(-i) + round(f) + floor(f) + ceil(f)"), 7 + 3 + 2 + 3);
    assert.equal(await one("round(2.345, 2)"), 2.35);
  });

  test("casts", async () => {
    assert.equal(await one("cast(f as int)"), 2);
    assert.equal(await one("cast(i as string)"), "7");
    assert.equal(await one("cast('12' as int) + 1"), 13);
    await assert.rejects(one("cast('x' as int)"), AnalysisError);
    await assert.rejects(one("cast(s as int)"), ExecutionError);
    assert.equal(await one("cast(f as string)"), "2.5");
    assert.equal(await one("cast(1 as float)"), 1);
  });

  test("type errors name the operator and the position", async () => {
    await assert.rejects(one("i + s"), (e: AnalysisError) => e instanceof AnalysisError && /'\+'/.test(e.message) && e.span?.[0] === 7);
    await assert.rejects(one("upper(i)"), AnalysisError);
    await assert.rejects(one("i and b"), AnalysisError);
    await assert.rejects(one("case when i then 1 end"), AnalysisError);
    await assert.rejects(one("case when b then 1 else 'x' end"), AnalysisError);
    await assert.rejects(one("nosuch(i)"), AnalysisError);
  });
});
