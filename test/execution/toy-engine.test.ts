// The shared layers run an engine they do not know (ADR 0001, criterion 5): a toy
// engine, registered only here, compiles, steps, runs, fails a stopped node's unit and
// returns its rows through Database.compile and the Session, with no kostka code
// involved (only the toy engine is registered).
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { openNodeSqlite } from "../../src/catalog/node-sqlite.ts";
import { MemoryFileStore } from "../../src/storage/file-store.ts";
import { Database } from "../../src/api/database.ts";
import { Engines } from "../../src/execution/registry.ts";
import { Session } from "../../src/ui/session.ts";
import { toy } from "../helpers/toy-engine.ts";

/** A Database with only the toy engine, and a table to compile queries against. */
async function toyDb(): Promise<Database> {
  const db = new Database(openNodeSqlite(), new MemoryFileStore(), new Engines([toy]));
  await db.createTable("t", [["x", "int"]]);
  return db;
}

describe("a toy engine through the shared layers", () => {
  test("Database.compile: it steps and runs, one unit per node", async () => {
    const db = await toyDb();
    const q = db.compile("select x from t", { cluster: { nodes: 3, cpu: "model" } });
    assert.equal(q.engine.name, "toy");
    const e = q.start();
    assert.equal(e.engine, "toy");
    assert.equal(e.explain(), "toy: one unit on each of 3 nodes");
    assert.equal(e.events[0]!.kind, "start");
    assert.ok(await e.step());
    assert.deepEqual(await e.run(), [[0], [1], [2]]);
    assert.deepEqual(e.events.filter((s) => s.kind === "work").map((s) => s.node), [0, 1, 2]);
    assert.equal((await db.query("select x from t", { cluster: { nodes: 2 } })).rows.length, 2);
  });

  test("a stopped node's unit fails and runs again on a node that is up: the same rows", async () => {
    const e = (await toyDb()).compile("select x from t", { cluster: { nodes: 2, cpu: "model" } }).start();
    await e.step(); // N0 ran unit 0; its rows are on the way
    assert.equal(e.events[1]!.kind, "work");
    e.stopNode(0);
    assert.deepEqual(await e.run(), [[0], [1]]);
    const st = e.state();
    assert.equal(st.nodes[0]!.status, "gone");
    assert.ok(st.units.some((u) => u.node === 0 && u.failed !== null), "the unit on N0 failed");
    assert.ok(st.units.some((u) => u.label === "unit 0" && u.node === 1 && u.failed === null), "and ran again on N1");
  });

  test("every node stopped fails the query", async () => {
    const e = (await toyDb()).compile("select x from t", { cluster: { nodes: 2 } }).start();
    await e.step();
    e.stopNode(0);
    e.stopNode(1);
    await assert.rejects(e.run(), /every node is gone/);
  });

  test("the Session: compile, step, stop a node, run; every state has the shared fields and is plain data", async () => {
    const s = new Session((await toyDb()));
    assert.deepEqual(s.info().engines.map((x) => x.name), ["toy"]);
    const c = s.compile("q", "select x from t", { cluster: { nodes: 2, cpu: "model" } });
    assert.equal(c.plans.engine, "toy");
    assert.match(c.plans.explain, /toy/);
    const n = await s.next("q", c.at, 0); // the engine's unit steps: "work"
    assert.equal(n.state.event.kind, "work");
    const gone = s.stopNode("q", 1, 0);
    assert.ok(gone.events.some((e) => e.kind === "node_gone"));
    const r = await s.run("q", 0);
    assert.ok(r.state.done);
    assert.deepEqual(r.state.result, { rows: [[0], [1]], total: 2 });
    for (let at = 0; at < r.total; at++) {
      const st = s.state("q", at);
      for (const k of ["engine", "at", "events", "event", "done", "failed", "elapsed_ms", "next", "cluster", "nodes", "units", "result_rows", "error", "result"]) assert.ok(k in st, `${k} at step ${at}`);
      assert.equal(st.engine, "toy");
      assert.deepEqual(structuredClone(st), st);
    }
  });
});
