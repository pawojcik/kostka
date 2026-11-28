// The Spark-style executor, with fake ports instead of the cluster (ADR 0002): a map
// task writes one shuffle file and reports its MapStatus; a reduce task reads its
// partition of every file; a broadcast is fetched and built once per executor and
// shared by its tasks; a task's memory is memoryMB / cores; a failed task leaves
// nothing behind; RemoveShuffle and RemoveBroadcast free what they name.
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import type { Database } from "../../../src/api/database.ts";
import type { Row } from "../../../src/core/types.ts";
import { Executor, type LaunchTask } from "../../../src/engines/spark/executor/executor.ts";
import type { ExecutorPorts, RowBlock, TaskClock } from "../../../src/engines/spark/executor/ports.ts";
import { TaskMemory } from "../../../src/engines/spark/executor/task-memory.ts";
import { planSpark } from "../../../src/engines/spark/physical/planner.ts";
import { cutStages, type StagePlan } from "../../../src/engines/spark/physical/stages.ts";
import type { ExecutorEvent, ShuffleInput } from "../../../src/engines/spark/protocol.ts";
import { tables } from "../../helpers/db.ts";

/** Ports that keep blocks and memory in maps and time nothing. */
function fakePorts(node: { blocks: Map<number, RowBlock>; held: Map<string, number>; fetched: number[]; network: number[] }, others: Map<number, Map<number, RowBlock>> = new Map()): ExecutorPorts {
  let next = 0;
  const clock = (): TaskClock => ({
    measured: false, cpu() {}, cpuTaken() {}, readStorage() {}, writeDisk() {}, readDisk() {}, network: (b) => { node.network.push(b); },
    end: () => ({ start: 0, end: 1, time: { cpu: 0, storage: 0, network: 0, spill_write: 0, spill_read: 0 }, segments: [] }),
  });
  return {
    begin: () => clock(),
    readBlock: (n, id) => {
      const b = (others.get(n) ?? node.blocks).get(id);
      return b ? { kind: "ok", rows: b.rows } : { kind: "missing" };
    },
    fetchBroadcast: (b) => {
      node.fetched.push(b);
      return [[1, "one"], [2, "two"]];
    },
    memory: {
      hold: (_s, key, _k, _l, bytes) => { node.held.set(key, bytes); },
      grow: (_s, key, bytes) => { node.held.set(key, bytes); },
      holdOnDisk: (_s, key, _k, _l, block) => { node.held.set(key, block.bytes); block.spilled = true; },
      release: (key) => { node.held.delete(key); },
    },
    blocks: {
      put: (rows: readonly Row[]) => {
        const b: RowBlock = { id: next++, rows, bytes: rows.length * 10, spilled: false };
        node.blocks.set(b.id, b);
        return b;
      },
      drop: (id) => { node.blocks.delete(id); },
    },
  };
}

describe("Spark-style executor", () => {
  let db: Database;
  before(async () => {
    db = await tables({
      t: { columns: [["k", "int"], ["v", "int"]], rows: Array.from({ length: 40 }, (_, i) => [i % 2 ? 1 : 2, i]) },
      d: { columns: [["k", "int"], ["name", "string"]], rows: [[1, "one"], [2, "two"]] },
    }, 10);
  });

  const plan = (sql: string, broadcastThreshold = -1): StagePlan => cutStages(planSpark(db.compile(sql).optimized, db.reader, { broadcastThreshold, shufflePartitions: 2 }));
  const launch = (stage: number, partition: number, shuffles: ShuffleInput[] = [], broadcasts: readonly number[] = [], tid = 0): LaunchTask =>
    ({ type: "LaunchTask", executor: 0, tid, stage, stageAttempt: 0, partition, attempt: 0, slot: 0, shuffles, broadcasts });
  const event = (e: Executor): ExecutorEvent => {
    const x = e.outbox.shift();
    assert.ok(x, "an event");
    return x.event;
  };

  test("a map task writes one shuffle file; a reduce task reads its partition of it", async () => {
    const p = plan("select k, count(*) from t group by k");
    const node = { blocks: new Map<number, RowBlock>(), held: new Map<string, number>(), fetched: [], network: [] };
    const e = new Executor(0, p, 2, 1 << 20, 64, fakePorts(node));
    e.handle(launch(0, 0), 0);
    assert.deepEqual(e.waiting().map((w) => w.slot), [0]);
    await e.prepare(e.slots[0]!);
    e.run(0, 0);
    const done = event(e);
    assert.equal(done.type, "TaskFinished");
    const ms = done.type === "TaskFinished" ? done.mapStatus : null;
    assert.ok(ms, "a MapStatus");
    assert.equal(ms.offsets.length, 3, "an index: where each of 2 partitions starts, and the end");
    assert.equal(e.blocks.state().shuffle_files.length, 1);
    assert.deepEqual([...node.held.keys()].filter((k) => k.startsWith("task:")), [], "the task's memory is freed");
    // the reduce side: each of 2 partitions reads its rows of the file
    let rows = 0;
    for (const part of [0, 1]) {
      e.handle(launch(1, part, [{ shuffle: 0, maps: [ms] }], [], 1 + part), 1);
      e.run(0, 1);
      const r = event(e);
      assert.equal(r.type, "TaskFinished");
      if (r.type === "TaskFinished") rows += r.rows?.length ?? 0;
    }
    assert.equal(rows, 2, "the two keys, one per partition or both in one");
    e.handle({ type: "RemoveShuffle", executor: 0, shuffle: 0 }, 2);
    assert.equal(e.blocks.state().shuffle_files.length, 0);
    assert.deepEqual([...node.held.keys()].filter((k) => k.startsWith("shuffle:")), []);
  });

  test("a missing shuffle location fails the task with FetchFailed", () => {
    const p = plan("select k, count(*) from t group by k");
    const node = { blocks: new Map<number, RowBlock>(), held: new Map<string, number>(), fetched: [], network: [] };
    const e = new Executor(0, p, 1, 1 << 20, 64, fakePorts(node));
    e.handle(launch(1, 0, [{ shuffle: 0, maps: [null] }]), 0);
    e.run(0, 0);
    const f = event(e);
    assert.equal(f.type, "TaskFailed");
    if (f.type === "TaskFailed") assert.deepEqual(f.fetchFailed, { shuffle: 0, map: 0, node: null });
  });

  test("a broadcast is fetched and built once per executor, and shared by its tasks", async () => {
    const p = plan("select name, count(*) from t join d on t.k = d.k group by name", 1 << 20);
    const probe = p.stages.find((s) => s.broadcasts.length)!;
    const node = { blocks: new Map<number, RowBlock>(), held: new Map<string, number>(), fetched: [] as number[], network: [] };
    const e = new Executor(0, p, 1, 1 << 20, 64, fakePorts(node));
    for (const part of [0, 1, 2]) {
      e.handle(launch(probe.id, part, [], probe.broadcasts, part), part);
      await e.prepare(e.slots[0]!);
      e.run(0, part);
      assert.equal(event(e).type, "TaskFinished");
    }
    assert.deepEqual(node.fetched, [probe.broadcasts[0]], "fetched once");
    assert.equal(e.blocks.state().broadcasts[0]!.hits, 2);
    e.handle({ type: "RemoveBroadcast", executor: 0, broadcast: probe.broadcasts[0]! }, 9);
    assert.deepEqual(e.blocks.state().broadcasts, []);
    assert.deepEqual([...node.held.keys()].filter((k) => k.startsWith("broadcast:")), []);
  });

  test("a task's memory: its share and no more; a failed task leaves nothing behind", async () => {
    const held = new Map<string, number>();
    const mem = new TaskMemory(100, { hold: (_s, k, _k, _l, b) => { held.set(k, b); }, grow: (_s, k, b) => { held.set(k, b); }, holdOnDisk() {}, release: (k) => { held.delete(k); } }, { spillWrite() {} }, "task:0", "S0 task 0.0");
    assert.ok(mem.reserve(1, "groups", 60));
    assert.ok(!mem.reserve(2, "sort buffer", 50), "60 + 50 is past the share of 100");
    assert.ok(mem.reserve(2, "sort buffer", 40));
    assert.equal(mem.peak, 100);
    mem.close();
    assert.equal(held.size, 0);
    // an aggregate too big for its share: the task fails, and its memory is freed
    const p = plan("select v, count(*) from t group by v");
    const node = { blocks: new Map<number, RowBlock>(), held: new Map<string, number>(), fetched: [], network: [] };
    const e = new Executor(0, p, 1, 64, 64, fakePorts(node));
    e.handle(launch(0, 0), 0);
    await e.prepare(e.slots[0]!);
    e.run(0, 0);
    const f = event(e);
    assert.equal(f.type, "TaskFailed");
    if (f.type === "TaskFailed") assert.match(f.error, /^OutOfMemoryError: HashAggregate \(partial\) needs/);
    assert.equal(node.held.size, 0);
    assert.equal(node.blocks.size, 0);
  });
});
