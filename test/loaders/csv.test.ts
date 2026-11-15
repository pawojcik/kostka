import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { csvRows, formatParser, inferCsv, parseCsv } from "../../src/loaders/csv.ts";
import { openDatabase } from "../../src/engine/node.ts";
import { SchemaError } from "../../src/core/errors.ts";
import { daysOf } from "../../src/core/types.ts";
import { generateRows, defaultGenerator } from "../../src/loaders/generators.ts";

describe("CSV", () => {
  test("parses quotes, embedded delimiters and newlines, CRLF", () => {
    assert.deepEqual(parseCsv('a,b\r\n"x,1","he said ""hi""\nbye"\n,\n'), [["a", "b"], ["x,1", 'he said "hi"\nbye'], ["", ""]]);
    assert.deepEqual(parseCsv("a;b\n1;2", ";"), [["a", "b"], ["1", "2"]]);
    assert.throws(() => parseCsv('"open'), SchemaError);
  });

  test("infers types from all values and makes valid names", () => {
    const inf = inferCsv("Id,Price,Name,OK,Day,At,Id\n1,2.5,x,true,2024-01-02,2024-01-02 10:00:00,1\n2,3,NULL,false,2024-01-03,2024-01-02T11:00:00,x\n");
    assert.deepEqual(inf.columns.map((c) => [c.name, c.type]), [["id", "int"], ["price", "float"], ["name", "string"], ["ok", "bool"], ["day", "date"], ["at", "timestamp"], ["id_2", "string"]]);
    assert.equal(inf.rows, 2);
    assert.deepEqual(inf.sample[1]!.slice(0, 4), [2, 3, null, false]);
  });

  test("imports with chosen columns, and appends by name", async () => {
    const db = openDatabase();
    const text = "a,b,c\n1,x,2024-01-02\n2,,2024-01-03\n";
    const cols = inferCsv(text).columns;
    cols[1]!.include = false;
    assert.equal(await db.importCsv(text, "t", "create", cols), 2);
    assert.deepEqual((await db.query("select * from t order by a")).rows, [[1, daysOf(2024, 1, 2)], [2, daysOf(2024, 1, 3)]]);
    await db.importCsv("a\n3\n", "t", "append");
    assert.deepEqual((await db.query("select a, c from t order by a")).rows.at(-1), [3, null]);
    await assert.rejects(db.importCsv("zz\n1\n", "t", "append"), SchemaError);
    const bad = inferCsv(text).columns.map((c) => ({ ...c, type: "int" as const }));
    await assert.rejects(db.importCsv(text, "u", "create", bad), /line 2/);
    assert.ok(!db.tableExists("u"));
  });

  test("reads a file with more rows than fit on the call stack", () => {
    const inf = inferCsv("a,b\n" + "1,x\n".repeat(300_000));
    assert.equal(inf.rows, 300_000);
    assert.deepEqual(inf.columns.map((c) => c.type), ["int", "string"]);
  });

  test("reads decimal commas and thousands separators", () => {
    const text = "n;x;s\n1.234;1.234,5;1,5\n7;-0,25;x\n";
    const o = { delimiter: ";", decimal: "," as const, thousands: "." as const };
    const inf = inferCsv(text, o);
    assert.deepEqual(inf.columns.map((c) => c.type), ["int", "float", "string"]);
    assert.deepEqual([...csvRows(text, inf.columns, o)], [[1234, 1234.5, "1,5"], [7, -0.25, "x"]]);
    // A misplaced separator isn't a number; a period isn't a decimal point here.
    assert.equal(inferCsv("x\n1.5\n12.34,5\n", { decimal: ",", thousands: "." }).columns[0]!.type, "string");
    assert.equal(inferCsv("x\n1.5\n", { decimal: "," }).columns[0]!.type, "string");
    assert.equal(inferCsv("x\n\"1,234.5\"\n\"12\"\n", { thousands: "," }).columns[0]!.type, "float");
    assert.throws(() => inferCsv("x\n1\n", { decimal: ",", thousands: "," }), SchemaError);
  });

  test("reads dates and timestamps in a custom format", () => {
    const text = "d,t\n17.05.2024,5/17/2024 1:45:30 PM\n01.01.2000,12/1/2024 12:00:00 AM\n";
    const o = { dateFormat: "DD.MM.YYYY", timestampFormat: "M/D/YYYY h:mm:ss A" };
    const inf = inferCsv(text, o);
    assert.deepEqual(inf.columns.map((c) => c.type), ["date", "timestamp"]);
    assert.deepEqual([...csvRows(text, inf.columns, o)], [
      [daysOf(2024, 5, 17), Date.UTC(2024, 4, 17, 13, 45, 30) * 1000],
      [daysOf(2000, 1, 1), Date.UTC(2024, 11, 1, 0, 0, 0) * 1000],
    ]);
    // Without the format these are strings; a date that doesn't exist doesn't match.
    assert.deepEqual(inferCsv(text).columns.map((c) => c.type), ["string", "string"]);
    assert.equal(inferCsv("d\n31.02.2024\n", o).columns[0]!.type, "string");
    const p = formatParser("DD MMM YY HH:mm:ss.SSS Z");
    assert.equal(p("03 Feb 24 10:00:00.250 +01:00"), Date.UTC(2024, 1, 3, 9, 0, 0, 250) * 1000);
    assert.equal(p("03 Foo 24 10:00:00.250 +01:00"), null);
    assert.throws(() => formatParser("HH:mm"), SchemaError);
  });
});

describe("generators", () => {
  test("defaults follow names and types; output is seeded", () => {
    assert.equal(defaultGenerator("id", "int").kind, "sequence");
    assert.equal(defaultGenerator("city", "string").kind, "choice");
    assert.equal(defaultGenerator("user_uuid", "string").kind, "uuid");
    const cols = [{ name: "id", type: "int" as const }, { name: "d", type: "date" as const }, { name: "u", type: "string" as const }];
    const gens = { u: { kind: "uuid" as const } };
    const a = generateRows(cols, 50, gens, 7, 10, "d"), b = generateRows(cols, 50, gens, 7, 10, "d");
    assert.deepEqual(a, b);
    assert.ok(a.every((r) => /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(r[2] as string)));
    const ds = a.map((r) => r[1]).filter((x) => x !== null) as number[];
    assert.deepEqual(ds, [...ds].sort((x, y) => x - y));
    assert.deepEqual(a.map((r) => r[0]).sort((x, y) => (x as number) - (y as number)), Array.from({ length: 50 }, (_, i) => 11 + i));
  });

  test("generate creates, appends and continues sequences", async () => {
    const db = openDatabase();
    await db.generate("t", 10, { columns: [["id", "int"], ["x", "float"]], seed: 1 });
    await db.generate("t", 5, { seed: 2 });
    assert.deepEqual((await db.query("select min(id), max(id), count(*) from t")).rows, [[1, 15, 15]]);
    await assert.rejects(db.generate("nope", 1), /does not exist/);
  });
});
