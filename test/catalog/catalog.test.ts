import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Catalog, CommitConflict } from "../../src/catalog/catalog.ts";
import { openNodeSqlite } from "../../src/catalog/node-sqlite.ts";
import { CatalogError, SchemaError, TableExistsError, TableNotFoundError } from "../../src/core/errors.ts";
import { columnStats } from "../../src/storage/stats.ts";

const file = (path: string, rows: number, values: number[] = []) => ({ path, rowCount: rows, sizeBytes: rows * 8, stats: new Map([["a", columnStats("int", values)]]) });

describe("catalog", () => {
  test("create, list, load, drop", () => {
    const c = new Catalog(openNodeSqlite());
    const m = c.createTable("t", [{ name: "a", type: "int" }, { name: "b", type: "string" }], "t-1");
    assert.equal(m.version, 0);
    assert.deepEqual(m.columns.map((x) => x.type), ["int", "string"]);
    assert.equal(m.location, "t-1");
    assert.deepEqual(c.listTables(), ["t"]);
    assert.ok(c.tableExists("t") && !c.tableExists("u") && !c.tableExists("bad name"));
    assert.throws(() => c.createTable("t", [{ name: "a", type: "int" }], "x"), TableExistsError);
    assert.throws(() => c.createTable("u", [], "x"), SchemaError);
    assert.throws(() => c.createTable("u", [{ name: "a", type: "int" }, { name: "a", type: "int" }], "x"), SchemaError);
    assert.throws(() => c.createTable("1u", [{ name: "a", type: "int" }], "x"), SchemaError);
    assert.equal(c.dropTable("t"), "t-1");
    assert.throws(() => c.loadTable("t"), TableNotFoundError);
    assert.throws(() => c.dropTable("t"), TableNotFoundError);
  });

  test("commits, files per snapshot, stats round-trip, revert", () => {
    const c = new Catalog(openNodeSqlite());
    let m = c.createTable("t", [{ name: "a", type: "int" }], "t", [file("f0", 2, [1, 2])], 2);
    const v1 = c.commit(m, "append", 3, [{ file: file("f1", 3, [5, 6, 7]), position: null }], []);
    assert.equal(v1.version, 1);
    m = c.loadTable("t");
    const files = c.files(m);
    assert.deepEqual(files.map((f) => [f.path, f.position]), [["f0", 0], ["f1", 1]]);
    const s = files[1]!.stats.get("a")!;
    assert.deepEqual([s.min, s.max, s.distinct, s.nullCount], [5, 7, 3, 0]);
    assert.ok(Math.round(s.sketch!.estimate()) === 3);
    c.commit(m, "replace", 1, [{ file: file("f2", 1, [9]), position: null }], files, [{ name: "a", type: "int" }, { name: "b", type: "string" }]);
    m = c.loadTable("t");
    assert.deepEqual(c.files(m).map((f) => f.path), ["f2"]);
    assert.deepEqual(c.files(m, 1).map((f) => f.path), ["f0", "f1"]);
    assert.equal(m.columns.length, 2);
    assert.equal(m.snapshot(1)!.columns.length, 1);
    const r = c.revert("t", 1);
    m = c.loadTable("t");
    assert.equal(r.version, 3);
    assert.equal(r.rows, 5);
    assert.equal(m.columns.length, 1);
    assert.deepEqual(c.files(m).map((f) => f.path), ["f0", "f1"]);
    assert.equal(c.files(m)[1]!.stats.get("a")!.max, 7);
    assert.throws(() => c.revert("t", 9), CatalogError);
    assert.throws(() => c.files(m, 9), CatalogError);
    assert.ok(m.snapshots.every((x, i) => i === 0 || x.createdUs > m.snapshots[i - 1]!.createdUs));
  });

  test("a commit conflicts when a file it removes is gone or the schema changed", () => {
    const c = new Catalog(openNodeSqlite());
    const m = c.createTable("t", [{ name: "a", type: "int" }], "t", [file("f0", 1)], 1);
    const f0 = c.files(m)[0]!;
    c.commit(m, "append", 1, [{ file: file("f1", 2), position: null }], [f0]);
    assert.throws(() => c.commit(m, "append", 1, [{ file: file("f2", 1), position: null }], [f0]), CommitConflict);
    c.commit(c.loadTable("t"), "replace", 0, [], [], [{ name: "x", type: "string" }]);
    assert.throws(() => c.commit(m, "append", 1, [], []), CommitConflict);
    assert.equal(c.loadTable("t").version, 2); // nothing half-committed
  });

  test("settings", () => {
    const c = new Catalog(openNodeSqlite());
    assert.equal(c.getSetting("file_rows", 8192), 8192);
    c.setSetting("file_rows", 100);
    c.setSetting("file_rows", 200);
    assert.equal(c.getSetting("file_rows", 8192), 200);
  });
});
