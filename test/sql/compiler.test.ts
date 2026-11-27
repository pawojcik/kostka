import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { AnalysisError, ParseError } from "../../src/core/errors.ts";
import { parse } from "../../src/sql/parser.ts";
import { explain } from "../../src/sql/plan.ts";
import type { Database } from "../../src/api/database.ts";
import { tables } from "../helpers/db.ts";

let db: Database;
before(async () => {
  db = await tables({
    f: { columns: [["k", "int"], ["v", "float"], ["s", "string"]], rows: Array.from({ length: 100 }, (_, i) => [i % 50, i * 1.5, `s${i % 7}`]) },
    d: { columns: [["k", "int"], ["name", "string"]], rows: Array.from({ length: 50 }, (_, i) => [i, `n${i}`]) },
    tiny: { columns: [["k", "int"]], rows: [[1], [2]] },
  }, 10);
});
const plan = (sql: string, rules?: string[]) => explain(db.compile(sql, rules ? { rules } : {}).optimized);
const fails = (sql: string, cls: typeof ParseError | typeof AnalysisError, re: RegExp, at?: number) =>
  assert.throws(() => db.compile(sql), (e: ParseError) => e instanceof cls && re.test(e.message) && (at === undefined || e.span?.[0] === at), sql);

describe("parser", () => {
  test("syntax errors point at the token", () => {
    fails("select from f", ParseError, /expected/i, 7);
    fails("select k from f where", ParseError, /expected/i);
    fails("select k, from f", ParseError, /expected/i);
    fails("select 'open from f", ParseError, /string/i, 7);
    fails("select k from f union select k from d", ParseError, /union|expected/i);
  });
  test("keywords are case-insensitive and quoted names keep their case", () => {
    assert.ok(parse('SELECT "k" FROM f WHERE k <> 1 -- comment\n ORDER BY k'));
  });
  test("date and timestamp are names unless a literal, a call or time travel follows", () => {
    assert.ok(parse("select date.d, timestamp from date, t where date.d = date '2024-01-01' and timestamp < timestamp('2024-01-01')"));
    assert.ok(parse("select x from t for timestamp as of '2024-05-17 12:00:00'"));
    assert.ok(parse("select cast(x as date) d from t"));
  });
});

describe("analyzer", () => {
  test("unknown and ambiguous names", () => {
    fails("select nope from f", AnalysisError, /unknown column 'nope'/, 7);
    fails("select k from f, d", AnalysisError, /ambiguous/, 7);
    fails("select x.k from f", AnalysisError, /unknown/);
    fails("select k from nope", AnalysisError, /nope/, 14);
    fails("select k from f for version as of 9", AnalysisError, /version 9/);
    fails("select f.k from f join f on f.k = f.k", AnalysisError, /twice|alias|ambiguous/);
  });
  test("aggregates and grouping", () => {
    fails("select k, sum(v) from f", AnalysisError, /group by|aggregate/i);
    fails("select sum(sum(v)) from f", AnalysisError, /nested|aggregate/i);
    fails("select k from f where sum(v) > 1", AnalysisError, /where|aggregate/i);
    fails("select sum(s) from f", AnalysisError, /sum/);
    fails("select k from f group by k having v > 1", AnalysisError, /v/);
    fails("select k, sum(v) n, count(*) n from f group by k having n > 1", AnalysisError, /HAVING n is ambiguous/);
  });
  test("types: only int->float and date->timestamp widen", () => {
    fails("select k from f where s = 1", AnalysisError, /'='/);
    fails("select k from f where k", AnalysisError, /bool/);
    fails("select case when k > 1 then 1 else 's' end from f", AnalysisError, /case|type/i);
    assert.ok(db.compile("select k + v, k = 1.0, coalesce(k, v) from f"));
    const cols = db.compile("select k / 2 a, k * 2 b, avg(k) c, count(*) n from f group by k").columns;
    assert.deepEqual(cols.map((c) => c.type), ["float", "int", "float", "int"]);
  });
  test("output names", () => {
    assert.deepEqual(db.compile("select k, k + 1, v as w, sum(v), (k + 1) * (k + 2) from f group by k, v").columns.map((c) => c.name), ["k", "k + 1", "w", "sum(v)", "(k + 1) * (k + 2)"]);
    assert.deepEqual(db.compile("select * from f join d on f.k = d.k").columns.map((c) => c.name), ["k", "v", "s", "k", "name"]);
  });
  test("ORDER BY may use hidden columns, output positions and aliases", async () => {
    assert.deepEqual((await db.query("select name from d order by k desc limit 2")).rows, [["n49"], ["n48"]]);
    assert.deepEqual((await db.query("select k, name x from d order by 2 desc, x limit 1")).rows, [[9, "n9"]]);
  });
  test("each table is pinned to one snapshot, also when read twice", async () => {
    const q = db.compile("select a.k from tiny a join tiny b on a.k = b.k");
    await db.append("tiny", [[3]]);
    const execution = q.start();
    assert.deepEqual((await execution.run()).map((r) => r[0]).sort(), [1, 2]);
  });
});

describe("optimizer", () => {
  const SQL = "select name, sum(v) from f join d on f.k = d.k where d.name = 'n3' and v > 10 group by name order by 2 desc limit 3";
  test("filters go below the join, to their own side", () => {
    const p = plan(SQL);
    assert.match(p, /Join inner on \(k#\d+ = k#\d+\)/);
    assert.match(p, /Filter \(v#\d+ > 10\.0\)[^\n]*\n\s+Scan f/);
    assert.match(p, /Filter \(name#\d+ = 'n3'\)[^\n]*\n\s+Scan d/);
  });
  test("file pruning reads only files that may match", () => {
    assert.match(plan(SQL), /Scan d@v\d+ files 1\/5/);
    assert.match(plan("select k from d where k between 12 and 17"), /files 1\/5/);
    assert.match(plan("select k from d where k < 0"), /files 0\/5/);
    assert.match(plan("select k from d where k <> 3"), /Scan d@v\d+ ->/);
    assert.match(plan("select k from d where k in (1, 45)"), /files 2\/5/);
  });
  test("projection pushdown reads only used columns; count(*) reads none", () => {
    assert.match(plan("select s from f"), /Scan f@v\d+ -> \[f\.s#\d+\]/);
    assert.match(plan("select count(*) from f"), /Scan f@v\d+ -> \[\]/);
  });
  test("ORDER BY + LIMIT becomes TopN", () => {
    assert.match(plan(SQL), /^TopN 3/);
    assert.doesNotMatch(plan(SQL, ["filter_pushdown"]), /TopN/);
  });
  test("join reorder puts the big table on the probe side", () => {
    const p = db.compile("select count(*) from tiny t, f, d where t.k = d.k and d.k = f.k");
    assert.ok(p.trace.some((t) => t.rule === "join_reorder"), p.trace.map((t) => t.rule).join());
  });
  test("with no rules the plan is the analyzer's, and results are the same", async () => {
    assert.equal(plan(SQL, []), explain(db.compile(SQL).logical));
    assert.deepEqual((await db.query(SQL, { rules: [] })).rows, (await db.query(SQL)).rows);
  });
});
