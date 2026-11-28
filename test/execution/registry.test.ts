// Choosing an engine (ADR 0001, criterion 3): by name, the first by default, and an
// unknown name fails with an error that names it and lists the engines there are.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { UnknownEngineError } from "../../src/core/errors.ts";
import { Engines } from "../../src/execution/registry.ts";
import { openDatabase } from "../../src/node.ts";
import { toy } from "../helpers/toy-engine.ts";
import { tables } from "../helpers/db.ts";

describe("the engines", () => {
  test("compile(sql, { engine: \"nope\" }) names nope and lists kostka and spark", async () => {
    const db = await tables({ t: { columns: [["x", "int"]], rows: [[1]] } });
    assert.throws(() => db.compile("select x from t", { engine: "nope" }),
      (e: Error) => e instanceof UnknownEngineError && /"nope"/.test(e.message) && /kostka, spark/.test(e.message));
    assert.equal(db.compile("select x from t", { engine: "spark" }).engine.name, "spark");
    assert.equal(db.compile("select x from t").engine.name, "kostka");
    assert.equal(db.compile("select x from t", { engine: "kostka" }).engine.name, "kostka");
  });

  test("openDatabase gives every engine; the first is the default", () => {
    assert.deepEqual(openDatabase().engines.names(), ["kostka", "spark"]);
    const e = new Engines([toy]);
    assert.equal(e.get(), toy);
    assert.equal(e.get("toy"), toy);
    assert.deepEqual(e.all(), [toy]);
  });

  test("a registry needs an engine, and one name each", () => {
    assert.throws(() => new Engines([]), /at least one engine/);
    assert.throws(() => new Engines([toy, toy]), /two engines are named toy/);
  });

  test("an engine checks its settings: kostka refuses one it does not have", async () => {
    const db = await tables({ t: { columns: [["x", "int"]], rows: [[1]] } });
    assert.throws(() => db.compile("select x from t", { settings: { slots: 4 } }).start(), /kostka has no setting "slots"/);
    assert.throws(() => db.compile("select x from t", { settings: { joinStrategy: "sideways" } }).start(), /joinStrategy must be one of/);
  });

  test("the cluster: workers is another name for nodes; the two must agree", async () => {
    const db = await tables({ t: { columns: [["x", "int"]], rows: [[1]] } });
    assert.equal(db.compile("select x from t", { cluster: { workers: 3 } }).cluster.nodes, 3);
    assert.throws(() => db.compile("select x from t", { cluster: { workers: 3, nodes: 2 } }));
  });
});
