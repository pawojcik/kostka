import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { openDatabase } from "../../src/node.ts";
import { AnalysisError, SchemaError, TableExistsError, TableNotFoundError } from "../../src/core/errors.ts";
import { formatTimestamp } from "../../src/core/types.ts";
import { MemoryFileStore } from "../../src/storage/file-store.ts";

const db = () => {
  const d = openDatabase();
  d.fileRows = 4;
  return d;
};
const all = async (d: ReturnType<typeof db>, t: string, v?: number) => (await d.query(`select * from ${t}${v === undefined ? "" : ` for version as of ${v}`} order by 1`)).rows;

describe("writing tables", () => {
  test("create is one version; rows are cut into files of file_rows", async () => {
    const d = db();
    await d.replace("t", [["a", "int"], ["b", "string"]], Array.from({ length: 10 }, (_, i) => [i, `v${i}`]));
    assert.equal(d.describe("t").version, 0);
    assert.deepEqual(d.files("t").map((f) => f.rowCount), [4, 4, 2]);
    assert.equal((await all(d, "t")).length, 10);
  });

  test("invalid rows write nothing", async () => {
    const d = db();
    await d.createTable("t", [["a", "int"]]);
    await assert.rejects(d.append("t", [[1], ["x"]]), SchemaError);
    await assert.rejects(d.append("t", [[1, 2]]), SchemaError);
    await assert.rejects(d.append("t", [{ nope: 1 }]), SchemaError);
    await assert.rejects(d.append("t", [[2 ** 53]]), SchemaError);
    assert.equal(d.describe("t").version, 0);
    assert.equal((d.store as MemoryFileStore).files.size, 0);
  });

  test("an empty append is a no-op; a missing table is an error", async () => {
    const d = db();
    await d.createTable("t", [["a", "int"]]);
    assert.equal(await d.append("t", []), 0);
    assert.equal(d.describe("t").version, 0);
    await assert.rejects(d.append("nope", [[1]]), TableNotFoundError);
    await assert.rejects(d.createTable("t", [["a", "int"]]), TableExistsError);
  });

  test("append compacts small files, unless told not to", async () => {
    const d = db();
    await d.replace("t", [["a", "int"]], [[1], [2], [3]]);
    await d.append("t", [[4], [5]]);
    assert.deepEqual(d.files("t").map((f) => f.rowCount), [4, 1]);
    await d.append("t", [[6]], false);
    assert.deepEqual(d.files("t").map((f) => f.rowCount), [4, 1, 1]);
    assert.deepEqual((await all(d, "t")).flat(), [1, 2, 3, 4, 5, 6]);
    assert.deepEqual((await all(d, "t", 1)).flat(), [1, 2, 3, 4, 5]);
  });

  test("replace changes the schema and keeps history; revert brings it back", async () => {
    const d = db();
    await d.replace("t", [["a", "int"]], [[1]]);
    await d.replace("t", [["s", "string"]], [["x"], ["y"]], "new");
    assert.deepEqual(await all(d, "t"), [["x"], ["y"]]);
    assert.deepEqual(await all(d, "t", 0), [[1]]);
    d.revert("t", 0);
    assert.deepEqual(await all(d, "t"), [[1]]);
    assert.deepEqual(d.history("t").map((h) => [h.version, h.operation, h.rows, h.files]), [[0, "create", 1, 1], [1, "replace", 2, 1], [2, "revert", 1, 1]]);
  });

  test("time travel by timestamp", async () => {
    const d = db();
    await d.replace("t", [["a", "int"]], [[1]]);
    const at = d.describe("t").current.createdUs;
    await d.append("t", [[2]]);
    assert.deepEqual((await d.query(`select a from t for timestamp as of '${formatTimestamp(at)}'`)).rows, [[1]]);
    assert.equal((await d.query(`select a from t for timestamp as of '${formatTimestamp(at + 10_000_000)}'`)).rows.length, 2);
    await assert.rejects(d.query(`select a from t for timestamp as of '${formatTimestamp(at - 1)}'`), AnalysisError);
  });

  test("concurrent appends both land (optimistic retry)", async () => {
    const d = db();
    await d.replace("t", [["a", "int"]], [[0]]);
    await Promise.all([d.append("t", [[1], [2]]), d.append("t", [[3]]), d.append("t", [[4], [5], [6]])]);
    assert.deepEqual((await all(d, "t")).flat(), [0, 1, 2, 3, 4, 5, 6]);
    assert.equal(d.describe("t").version, 3);
  });

  test("drop removes the folder; a new table gets a new folder", async () => {
    const d = db();
    await d.replace("t", [["a", "int"]], [[1]]);
    const loc = d.describe("t").location;
    await d.dropTable("t");
    assert.ok(![...(d.store as MemoryFileStore).files.keys()].some((p) => p.startsWith(loc)));
    await d.replace("t", [["a", "int"]], [[1]]);
    assert.notEqual(d.describe("t").location, loc);
  });

  test("file_rows is checked", () => {
    const d = db();
    assert.throws(() => (d.fileRows = 0), RangeError);
    assert.throws(() => (d.fileRows = 1.5), RangeError);
  });
});
