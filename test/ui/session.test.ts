import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { openDatabase } from "../../src/engine/node.ts";
import { Session } from "../../src/ui/session.ts";

describe("workbench session", () => {
  test("tables, compile, step and history; states are structured-cloneable", async () => {
    const s = new Session(openDatabase());
    await s.loadSample();
    assert.deepEqual(s.tables().map((t) => t.name), ["orders", "users"]);
    assert.deepEqual(Object.keys(s.schema()), ["orders", "users"]);
    const t = await s.table("users");
    assert.equal(t.rows.length, 50);
    assert.equal(t.total, 2000);
    assert.deepEqual(s.check("select nope from users"), [{ from: 7, to: 11, message: "unknown column 'nope'; available: users.id, users.name, users.city, users.active" }]);
    assert.deepEqual(s.check("select id from users"), []);
    const { plans, state } = s.compile("q0", "select city, count(*) n from users join orders on users.id = orders.user_id group by city order by n desc", { cluster: { workers: 2 } });
    assert.equal(plans.optimized.kind, "Sort");
    assert.ok(plans.steps.length > 0);
    structuredClone(plans);
    structuredClone(state);
    const c = s.compile("q1", "select city, count(*) n from users join orders on users.id = orders.user_id group by city order by n desc", { cluster: { workers: 2 } });
    assert.equal(c.events[0]!.kind, "start");
    let known = c.total;
    // Step moves one step on, running the simulation only when it must
    const a = await s.step("q1", c.at, known);
    assert.equal(a.at, c.at + 1);
    assert.equal((a.state as any).event.seq, a.at);
    known += a.events.length;
    const back = await s.step("q1", 0, known);
    assert.equal(back.at, 1);
    assert.equal(back.events.length, 0, "nothing ran: step 1 was already there");
    const sp = await s.next("q1", a.at, known);
    assert.equal((sp.state as any).event.kind, "process");
    known += sp.events.length;
    await s.wave("q1", known);
    const gone = await s.stopWorker("q1", 1, 0);
    assert.ok(gone.events.some((e: any) => e.kind === "worker_gone"));
    assert.equal((gone.state as any).members[1].status, "gone");
    const r = await s.run("q1", 0);
    assert.equal(r.events.length, r.total, "all steps, from the first");
    assert.ok(r.events.some((e: any) => e.messages.some((m: any) => m.type === "AddSplits")));
    const st = r.state as any;
    assert.ok(st.done);
    assert.equal(st.result.total, 13); // 12 cities and NULL
    assert.ok(st.pipelines.some((p: any) => p.ops.some((o: any) => o.node !== null)));
    structuredClone(st);
    const first = s.state("q1", 0) as any;
    assert.equal(first.units_run, 0);
    assert.equal(first.result.total, 0);
    structuredClone(first);
  });

  test("a failing query reports its error in the state", async () => {
    const s = new Session(openDatabase());
    await s.loadSample();
    s.compile("q", "select id + 9007199254740990 + id from users", {});
    const { state } = await s.run("q");
    assert.match((state as any).error, /overflow/);
  });
});
