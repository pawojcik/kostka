// The Spark-style driver, with no simulation (ADR 0002): the TaskScheduler (slots,
// most free slots first, retries), the MapOutputTracker (locations, lost and removed
// outputs) and the DAGScheduler fed events by hand: stages as barriers, FetchFailed
// and the parent stage resubmitted for its missing partitions only, a task that fails
// maxTaskFailures times, and every executor lost.
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import type { Database } from "../../../src/api/database.ts";
import { DAGScheduler, type Decision } from "../../../src/engines/spark/driver/dag-scheduler.ts";
import { MapOutputTracker } from "../../../src/engines/spark/driver/map-output-tracker.ts";
import { TaskScheduler } from "../../../src/engines/spark/driver/task-scheduler.ts";
import { planSpark } from "../../../src/engines/spark/physical/planner.ts";
import { cutStages, type StagePlan } from "../../../src/engines/spark/physical/stages.ts";
import type { Command, MapStatus, TaskMetrics } from "../../../src/engines/spark/protocol.ts";
import { tables } from "../../helpers/db.ts";

const status = (node: number, n: number): MapStatus => ({ node, block: 0, bytes: Array.from({ length: n }, () => 10), offsets: Array.from({ length: n + 1 }, (_, i) => i) });
const metrics: TaskMetrics = {
  start: 0, end: 0, time: { cpu: 0, storage: 0, network: 0, spill_write: 0, spill_read: 0 }, segments: [], inputBytes: 0, inputRows: 0, shuffleReadBytes: 0,
  shuffleReadRows: 0, shuffleWriteBytes: 0, shuffleWriteRows: 0, spillBytes: 0, peakMemory: 0, ops: [], fetched: [],
};

describe("TaskScheduler", () => {
  test("the executor with the most free slots first, its lowest free slot", () => {
    const ts = new TaskScheduler(2, 2);
    ts.submit(0, 0, 0, [0, 1, 2, 3, 4]);
    const l = ts.offer();
    assert.deepEqual(l.map((x) => [x.executor, x.slot, x.partition, x.attempt]), [[0, 0, 0, 0], [1, 0, 1, 0], [0, 1, 2, 0], [1, 1, 3, 0]]);
    assert.deepEqual(ts.offer(), [], "no free slot");
    ts.finished(l[1]!.tid);
    assert.deepEqual(ts.offer().map((x) => [x.executor, x.slot, x.partition]), [[1, 0, 4]]);
  });

  test("a failed task runs again with the next attempt; only counted failures count", () => {
    const ts = new TaskScheduler(1, 1);
    const set = ts.submit(0, 0, 0, [0]);
    const [a] = ts.offer();
    assert.equal(ts.failed(a!.tid, true), 1);
    const [b] = ts.offer();
    assert.equal(b!.attempt, 1);
    assert.equal(ts.failed(b!.tid, false), 1, "not counted");
    const [c] = ts.offer();
    assert.equal(c!.attempt, 2);
    ts.makeZombie(set);
    assert.equal(ts.failed(c!.tid, true), 2);
    assert.deepEqual(ts.offer(), [], "a zombie set launches nothing");
    assert.ok(ts.over(set));
  });

  test("an executor lost: its running tasks run again elsewhere, not counted", () => {
    const ts = new TaskScheduler(2, 1);
    const set = ts.submit(0, 0, 0, [0, 1]);
    ts.offer();
    assert.deepEqual(ts.executorLost(1), [1]);
    assert.equal(set.failures.get(1), 0);
    assert.deepEqual(ts.executorsUp(), [0]);
    assert.deepEqual(ts.offer(), [], "executor 0 is busy");
    ts.finished(0);
    assert.deepEqual(ts.offer().map((x) => [x.executor, x.partition, x.attempt]), [[0, 1, 1]]);
  });
});

describe("MapOutputTracker", () => {
  test("outputs: registered, lost with their node, removed; readers get null for a missing one", () => {
    const t = new MapOutputTracker([3]);
    assert.deepEqual(t.missing(0), [0, 1, 2]);
    t.register(0, 0, status(0, 2), 0, 1);
    t.register(0, 1, status(1, 2), 1, 2);
    t.register(0, 2, status(1, 2), 2, 3);
    assert.ok(t.available(0));
    assert.deepEqual(t.lostWith(1, 4), [{ shuffle: 0, map: 1 }, { shuffle: 0, map: 2 }]);
    assert.deepEqual(t.missing(0), [1, 2]);
    assert.deepEqual(t.inputs([0])[0]!.maps.map((m) => m?.node ?? null), [0, null, null]);
    assert.equal(t.at(0, 3)[1]?.state, "available", "the state after an earlier step");
    t.remove(0, 5);
    assert.deepEqual(t.missing(0), [0, 1, 2]);
  });
});

describe("DAGScheduler", () => {
  let db: Database;
  before(async () => {
    db = await tables({ t: { columns: [["k", "int"], ["v", "int"]], rows: Array.from({ length: 40 }, (_, i) => [i % 5, i]) } }, 10);
  });

  /** A driver for `sql` on `executors` × `slots`, its decisions recorded. */
  function driver(sql: string, executors: number, slots: number, maxTaskFailures = 4): { d: DAGScheduler; plan: StagePlan; steps: Decision[]; launches: () => Extract<Command, { type: "LaunchTask" }>[] } {
    const plan = cutStages(planSpark(db.compile(sql).optimized, db.reader, { broadcastThreshold: -1, shufflePartitions: 2 }));
    const steps: Decision[] = [];
    const d = new DAGScheduler(plan, { executors, slots, maxTaskFailures, maxStageAttempts: 4 }, { decide: (x) => steps.push(x), seq: () => steps.length });
    d.start(0);
    const launches = () => steps.flatMap((s) => s.commands.filter((c): c is Extract<Command, { type: "LaunchTask" }> => c.type === "LaunchTask"));
    return { d, plan, steps, launches };
  }

  type Launch = Extract<Command, { type: "LaunchTask" }>;
  const finish = (d: DAGScheduler, l: Launch, node = l.executor) =>
    d.on({ type: "TaskFinished", executor: l.executor, tid: l.tid, stage: l.stage, stageAttempt: l.stageAttempt, partition: l.partition, attempt: l.attempt, mapStatus: d.plan.stage(l.stage).writes ? status(node, 2) : null, rows: d.plan.stage(l.stage).writes ? null : [[l.partition]], metrics }, 1);

  test("stages are barriers: no task of stage 1 before every task of stage 0 finished", () => {
    const { d, steps, launches } = driver("select k, count(*) from t group by k", 2, 1);
    assert.deepEqual(steps.slice(0, 3).map((s) => s.kind), ["SQLExecutionStart", "JobStart", "StageSubmitted"]);
    // stage 0 scans 4 files: 2 slots, so 2 tasks now
    assert.deepEqual(launches().map((l) => l.stage), [0, 0]);
    finish(d, launches()[0]!);
    finish(d, launches()[1]!);
    assert.deepEqual(launches().map((l) => l.stage), [0, 0, 0, 0], "the last two scan tasks");
    finish(d, launches()[2]!);
    assert.ok(!launches().some((l) => l.stage === 1), "stage 1 waits for the last stage-0 task");
    finish(d, launches()[3]!);
    assert.ok(steps.some((s) => s.kind === "StageCompleted" && s.refs.stage === 0));
    const s1 = launches().filter((l) => l.stage === 1);
    assert.equal(s1.length, 2);
    for (const l of s1) assert.equal(l.shuffles[0]!.maps.length, 4, "each reduce task is told every map output");
    for (const l of s1) finish(d, l);
    assert.ok(d.done);
    assert.equal(steps.at(-1)!.kind, "SQLExecutionEnd");
    assert.ok(steps.some((s) => s.commands.some((c) => c.type === "RemoveShuffle")), "shuffle 0 is removed once stage 1 is done");
  });

  test("FetchFailed: the stage attempt fails, the parent runs again for its missing partitions only", () => {
    const { d, steps, launches } = driver("select k, count(*) from t group by k", 2, 2);
    for (const l of launches()) finish(d, l);
    assert.equal(d.stage(0).status.now, "done");
    const reduce = launches().filter((l) => l.stage === 1);
    assert.equal(reduce.length, 2);
    // executor 1 is lost: its 2 map outputs with it, and its running reduce task
    d.on({ type: "ExecutorLost", executor: 1 }, 2);
    assert.equal(d.stage(0).status.now, "waiting");
    const r0 = reduce.find((l) => l.executor === 0)!;
    d.on({ type: "TaskFailed", executor: 0, tid: r0.tid, stage: 1, stageAttempt: 0, partition: r0.partition, attempt: 0, error: "FetchFailed: gone", name: "FetchFailed", fetchFailed: { shuffle: 0, map: 1, node: 1 }, metrics: null }, 3);
    assert.ok(steps.some((s) => s.kind === "FetchFailed"));
    const re = steps.filter((s) => s.kind === "StageResubmitted");
    assert.equal(re.length, 1);
    assert.equal(re[0]!.refs.stage, 0);
    assert.match(re[0]!.text, /attempt 1, for 2 tasks: partitions 1, 3/);
    // The reduce task lost with executor 1 was launched again on executor 0 before the
    // FetchFailed: it fails too, and its stage attempt has failed already.
    const stale = launches().find((l) => l.stage === 1 && l.attempt === 1)!;
    assert.deepEqual(launches().filter((l) => l.stage === 0 && l.stageAttempt === 1).map((l) => [l.partition, l.executor]), [[1, 0]]);
    d.on({ type: "TaskFailed", executor: 0, tid: stale.tid, stage: 1, stageAttempt: 0, partition: stale.partition, attempt: 1, error: "FetchFailed: gone", name: "FetchFailed", fetchFailed: { shuffle: 0, map: 1, node: 1 }, metrics: null }, 3);
    assert.match(steps.at(-2)!.text, /has failed already/);
    const again = launches().filter((l) => l.stage === 0 && l.stageAttempt === 1);
    assert.deepEqual(again.map((l) => [l.partition, l.executor]), [[1, 0], [3, 0]]);
    for (const l of again) finish(d, l);
    const s1 = launches().filter((l) => l.stage === 1 && l.stageAttempt === 1);
    assert.deepEqual(s1.map((l) => l.partition), [0, 1], "stage 1 runs again as attempt 1");
    for (const l of s1) finish(d, l);
    assert.ok(d.done);
  });

  test("a task that fails maxTaskFailures times fails the query, naming it", () => {
    const { d, steps, launches } = driver("select k, count(*) from t group by k", 1, 1, 3);
    for (let i = 0; i < 3; i++) {
      const l = launches().at(-1)!;
      assert.equal(l.partition, 0);
      assert.equal(l.attempt, i);
      d.on({ type: "TaskFailed", executor: 0, tid: l.tid, stage: 0, stageAttempt: 0, partition: 0, attempt: i, error: "OutOfMemoryError: too big", name: "OutOfMemoryError", fetchFailed: null, metrics: null }, i + 1);
    }
    assert.equal(steps.at(-1)!.kind, "SQLExecutionFailed");
    assert.match(d.failed!.error, /^Task 0 in stage 0\.0 failed 3 times; most recent failure: lost task 0\.2 in stage 0\.0 \(TID \d+\) on executor 0: OutOfMemoryError: too big/);
  });

  test("every executor lost fails the query", () => {
    const { d } = driver("select k, count(*) from t group by k", 2, 1);
    d.on({ type: "ExecutorLost", executor: 0 }, 1);
    assert.equal(d.failed, null);
    d.on({ type: "ExecutorLost", executor: 1 }, 2);
    assert.match(d.failed!.error, /every node is gone: no executor can run stage 0/);
  });
});
