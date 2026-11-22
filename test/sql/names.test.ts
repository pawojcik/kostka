// Names are case-insensitive and case-preserving.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Catalog } from "../../src/catalog/catalog.ts";
import { openNodeSqlite } from "../../src/catalog/node-sqlite.ts";
import { AnalysisError, CatalogError, SchemaError, TableExistsError } from "../../src/core/errors.ts";
import { rows, tables } from "../helpers/db.ts";

const db = () => tables({
  Foo: { columns: [["a", "int"], ["Bar", "string"]], rows: [[1, "x"], [2, "y"], [2, "z"]] },
});

describe("names ignore case", () => {
  test("tables, columns, qualifiers and aliases in any case", async () => {
    const d = await db();
    assert.deepEqual(await rows(d, "select A, bar from foo order by A"), [[1, "x"], [2, "y"], [2, "z"]]);
    assert.deepEqual(await rows(d, "select f.a from FOO F where F.BAR = 'y'"), [[2]]);
    assert.deepEqual(await rows(d, `select "A" from "FOO" where a = 1`), [[1]]);
    assert.deepEqual(await rows(d, "select F.* from foo f where a = 1"), [[1, "x"]]);
    assert.deepEqual(await rows(d, "select a as N, count(*) as C from foo group by n having c > 1 order by N"), [[2, 2]]);
    assert.deepEqual(await rows(d, "with Q as (select a from foo) select q.A from q where A = 1"), [[1]]);
  });

  test("output columns keep the catalog's spelling unless aliased", async () => {
    const d = await db();
    assert.deepEqual((await d.query("select A, BAR, a as Big from foo limit 1")).columns.map((c) => c.name), ["a", "Bar", "Big"]);
    assert.equal(d.describe("FOO").name, "Foo");
  });

  test("names that differ only in case collide", async () => {
    const d = await db();
    await assert.rejects(d.query("select * from foo join FOO on true"), /'foo' and 'FOO' \(names ignore case\) appear twice in FROM/);
    await assert.rejects(d.query("select a from foo x join foo X on true"), /twice in FROM/);
    await assert.rejects(d.query("select A from foo x join foo y on x.a = y.A"), AnalysisError); // ambiguous
    await assert.rejects(d.query("with q as (select 1 as a from foo), Q as (select 2 as a from foo) select * from q"), /defined twice/);
    await assert.rejects(d.query("select nope from foo"), /unknown column 'nope'; available/);
  });

  test("the catalog refuses tables and columns that differ only in case", async () => {
    const d = await db();
    await assert.rejects(d.createTable("FOO", [["a", "int"]]), (e: unknown) => e instanceof TableExistsError && /already exists as 'Foo'/.test(e.message));
    await assert.rejects(d.createTable("t", [["id", "int"], ["ID", "int"]]), (e: unknown) => e instanceof SchemaError && /'id' and 'ID' differ only in case/.test(e.message));
    assert.ok(d.tableExists("FOO"));
    await d.dropTable("FOO");
    assert.deepEqual(d.listTables(), []);
  });

  test("appends match columns by name ignoring case", async () => {
    const d = await db();
    const w = await d.writer("foo");
    await w.addObjects([{ A: 3, bar: "w" }]);
    await assert.rejects(w.addObjects([{ a: 4, A: 5 }]), /given twice/);
    await w.commit();
    await d.importCsv("BAR,A\nv,4\n", "FOO", "append");
    await assert.rejects(d.importCsv("a,A\n1,2\n", "foo", "append"), SchemaError);
    assert.deepEqual(await rows(d, "select a, bar from foo where a > 2 order by a"), [[3, "w"], [4, "v"]]);
  });

  test("a catalog made before names ignored case fails to open on a clash", () => {
    const sql = openNodeSqlite();
    sql.exec("CREATE TABLE tables (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, dir TEXT NOT NULL, created_us INTEGER NOT NULL)");
    sql.exec("INSERT INTO tables (name, dir, created_us) VALUES ('foo', 'a', 0), ('FOO', 'b', 0), ('bar', 'c', 0)");
    assert.throws(() => new Catalog(sql), (e: unknown) => e instanceof CatalogError && /'foo', 'FOO'|'FOO', 'foo'/.test(e.message));
  });
});
