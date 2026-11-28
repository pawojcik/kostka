// The Spark-style engine on the simulated cluster (ADR 0002):
// - criterion 4: no task of a stage starts before every task of its parents (and of
//   the broadcast jobs it reads) has finished, over every SQL-vs-SQLite query;
// - criterion 5: with 1 node and 2 cores at most 2 tasks run at once, and a scan
//   stage of 4 files takes at most 60% of its time with 1 core;
// - criterion 6: a node stopped at 20 evenly spaced steps gives the same rows, with a
//   resubmitted stage (and FetchFailed when a reading stage was running);
// - criterion 7: a hash aggregate bigger than a task's memory fails after 4 attempts;
// - criterion 8: a sort bigger than a task's memory spills and is still right;
// - a broadcast hash table bigger than a node's memory, and shuffle files bigger
//   than its disk, fail the query with OutOfMemoryError and OutOfDiskError.
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import type { DatabaseSync } from "node:sqlite";
import { OutOfDiskError, OutOfMemoryError } from "../../../src/core/errors.ts";
import type { Value } from "../../../src/core/types.ts";
import type { Database } from "../../../src/api/database.ts";
import type { QueryOptions } from "../../../src/api/query.ts";
import { openDatabase } from "../../../src/node.ts";
import { SparkExecution } from "../../../src/engines/spark/sim.ts";
import { QUERIES as SSB } from "../../../src/loaders/ssb/queries.ts";
import { QUERIES as SAMPLE } from "../../../src/loaders/sample/queries.ts";
import { assertRows } from "../../helpers/compare.ts";
import { sqliteCopy, tables } from "../../helpers/db.ts";
import { SORTED, SPARK_CONFIGS, UNSORTED, sqlCaseTables } from "../../helpers/sql-cases.ts";

function spark(db: Database, sql: string, o: QueryOptions = {}): SparkExecution {
  const e = db.compile(sql, { ...o, engine: "spark" }).start();
  assert.ok(e instanceof SparkExecution, "a Spark-style execution");
  return e;
}

const key = (rows: readonly unknown[][]) => rows.map((r) => JSON.stringify(r)).sort().join("\n");
type Unit = { stage: number; start_ms: number; end_ms: number; node: number; core: number; failed: string | null; seq: number };

describe("Spark-style: stages are barriers", () => {
  let db: Database;
  before(async () => {
    db = await sqlCaseTables();
  });

  test("no task of a stage starts before every task of its parents has finished", async () => {
    let checked = 0;
    for (const sql of [...SORTED, ...UNSORTED]) {
      for (const o of SPARK_CONFIGS) {
        const e = spark(db, sql, o);
        await e.run();
        const units = e.state().units as unknown as Unit[];
        for (const s of e.stages.stages) {
          const mine = units.filter((u) => u.stage === s.id);
          // the stages it reads: its parents, and the stages of the broadcast jobs it reads
          const before = [...s.parents, ...s.broadcasts.flatMap((b) => e.stages.job(e.stages.maker.get(b)!).stages)];
          const theirs = units.filter((u) => before.includes(u.stage));
          if (!mine.length || !theirs.length) continue;
          const first = Math.min(...mine.map((u) => u.start_ms)), last = Math.max(...theirs.map((u) => u.end_ms));
          assert.ok(first >= last, `${sql} ${JSON.stringify(o)}: stage ${s.id} started at ${first} ms, before stage(s) ${before.join(", ")} ended at ${last} ms`);
          checked++;
        }
      }
    }
    assert.ok(checked > 300, `checked ${checked} stages`);
  });
});

describe("Spark-style: slots and cores", () => {
  let db: Database;
  before(async () => {
    db = await tables({ t: { columns: [["k", "int"], ["v", "float"]], rows: Array.from({ length: 4000 }, (_, i) => [i % 50, i * 0.5]) } }, 1000);
  });

  /** The most units that overlap in time. */
  const overlap = (units: readonly Unit[]) => {
    const ev = units.flatMap((u) => [[u.start_ms, 1], [u.end_ms, -1]] as const).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    let now = 0, most = 0;
    for (const [, d] of ev) most = Math.max(most, (now += d));
    return most;
  };

  test("1 node, 2 cores: at most 2 tasks at once, one per slot; the scan stage takes at most 60% of its time with 1 core", async () => {
    const sql = "select k, count(*), sum(v) from t group by k";
    const time = async (cores: number) => {
      const e = spark(db, sql, { cluster: { nodes: 1, cores, io: "none", cpu: "model" } });
      await e.run();
      const units = e.state().units as unknown as Unit[];
      const scan = e.stages.stages.find((s) => s.scans.length)!;
      assert.equal(scan.partitions, 4, "4 files");
      const mine = units.filter((u) => u.stage === scan.id);
      assert.equal(overlap(units), cores === 1 ? 1 : 2);
      assert.deepEqual([...new Set(units.map((u) => u.core))].sort(), cores === 1 ? [0] : [0, 1], "slot i runs on core i");
      return Math.max(...mine.map((u) => u.end_ms)) - Math.min(...mine.map((u) => u.start_ms));
    };
    const one = await time(1), two = await time(2);
    assert.ok(two <= 0.6 * one, `2 cores: ${two} ms, 1 core: ${one} ms`);
  });

  test("tasks on one node wait for its IO channel", async () => {
    const e = spark(db, "select k, count(*) from t group by k", { cluster: { nodes: 1, cores: 2, io: "slow", cpu: "model" } });
    await e.run();
    const units = e.state().units as unknown as { segments: [string, number, number][] }[];
    assert.ok(units.some((u) => u.segments.some(([k]) => k === "wait")), "a wait segment");
  });
});

describe("Spark-style: recovery", () => {
  const cases: [string, () => Promise<Database>, string][] = [
    ["SSB Q2.1", async () => { const db = openDatabase(); await db.loadSsb(0.01); return db; }, SSB.find((q) => q.id === "ssb2.1")!.sql],
    ["revenue per city", async () => { const db = openDatabase(); await db.loadSample(); return db; }, SAMPLE.find((q) => q.id === "s1")!.sql],
  ];
  let fetchFailures = 0;

  /** At the stop: did node `n` hold a shuffle output that a stage not done yet still
   * needs (so a stage is resubmitted), and is a stage that reads it running with a
   * partition left to run that reads a non-empty part of it (so it fails a fetch)? */
  function needs(e: SparkExecution, n: number): { resubmit: boolean; fetch: boolean } {
    const d = e.driver;
    let resubmit = false, fetch = false;
    for (const sh of e.plan.shuffles) {
      const lost = d.tracker.at(sh.shuffle, e.events.length - 1).filter((o) => o && o.state === "available" && o.status.node === n);
      if (!lost.length) continue;
      for (const r of e.stages.stages.filter((s) => s.reads.includes(sh.shuffle))) {
        const rs = d.stage(r.id);
        if (rs.status.now === "done") continue;
        if (rs.status.now !== "running") {
          resubmit = true;
          continue;
        }
        // A task that ran already read its input, even if its TaskFinished is on the way.
        const ran = new Set(e.units.filter((u) => !u.failed).map((u) => u.tid));
        const left = Array.from({ length: r.partitions }, (_, p) => p).filter((p) =>
          !d.tasks.some((t) => t.stage === r.id && t.partition === p && (t.status.now === "success" || ran.has(t.tid))));
        if (left.some((p) => lost.some((o) => (o!.status.bytes[p] ?? 0) > 0))) resubmit = fetch = true;
      }
    }
    return { resubmit, fetch };
  }

  for (const [name, load, sql] of cases) {
    test(`a node stopped at each of 20 steps: the same rows (${name}, 3 nodes)`, async () => {
      const db = await load();
      const o: QueryOptions = { cluster: { nodes: 3, cpu: "model" } };
      const clean = spark(db, sql, o);
      const want = await clean.run();
      const steps = clean.events.length;
      let resubmitted = 0;
      for (let i = 0; i < 20; i++) {
        const at = Math.floor((i * steps) / 20);
        const n = i % 3;
        const e = spark(db, sql, o);
        for (let k = 0; k < at && !e.done; k++) await e.step();
        if (e.done) continue;
        const need = needs(e, n);
        e.stopNode(n);
        const got = await e.run();
        assert.equal(key(got), key(want), `stopped node ${n} after ${at} steps`);
        const kinds = e.events.map((x) => x.kind);
        const gone = e.events.find((x) => x.kind === "ExecutorLost")!.seq;
        const units = e.state().units as unknown as Unit[];
        assert.ok(!units.some((u) => u.node === n && u.seq > gone), "nothing ran on the gone node after");
        if (need.resubmit) {
          assert.ok(e.events.some((x) => x.kind === "StageResubmitted" && /as attempt 1,/.test(x.text)), `stopped node ${n} after ${at} steps: a stage resubmitted as attempt 1`);
          resubmitted++;
        }
        if (need.fetch) assert.ok(kinds.includes("FetchFailed"), `stopped node ${n} after ${at} steps: FetchFailed`);
        if (kinds.includes("FetchFailed")) fetchFailures++;
      }
      assert.ok(resubmitted > 0, "some stop lost shuffle files a later stage needed");
    });
  }

  test("FetchFailed happened in some of those runs", () => {
    assert.ok(fetchFailures > 0, `${fetchFailures} runs with FetchFailed`);
  });

  test("every node stopped fails the query", async () => {
    const db = await cases[1]![1]();
    const e = spark(db, cases[1]![2], { cluster: { nodes: 2 } });
    for (let i = 0; i < 6; i++) await e.step();
    e.stopNode(0);
    assert.throws(() => e.stopNode(0), /gone already/);
    e.stopNode(1);
    await assert.rejects(e.run(), /every node is gone: no executor can run stage \d+/);
    assert.equal(e.events.at(-1)!.kind, "SQLExecutionFailed");
  });
});

describe("Spark-style: memory", () => {
  let db: Database;
  let sqlite: DatabaseSync;
  before(async () => {
    db = await tables({ t: { columns: [["id", "int"], ["s", "string"], ["v", "float"]], rows: Array.from({ length: 20000 }, (_, i) => [i, `name-${i % 977}`, (i * 7919) % 10007]) } }, 8192);
    sqlite = await sqliteCopy(db);
  });

  test("a hash aggregate bigger than memory / cores fails after 4 attempts of the same task", async () => {
    const e = spark(db, "select id, count(*) from t group by id", { cluster: { nodes: 1, cores: 2, memoryMB: 0.25, cpu: "model" } });
    await assert.rejects(e.run(), (err: Error) => {
      assert.ok(err instanceof OutOfMemoryError, `${err.name}: ${err.message}`);
      assert.match(err.message, /Task (\d+) in stage (\d+)\.0 failed 4 times; most recent failure: lost task \1\.3 in stage \2\.0 \(TID \d+\)/);
      assert.match(err.message, /more memory, use fewer cores, or use more shuffle partitions/);
      return true;
    });
    const failed = e.state().tasks as unknown as { stage: number; partition: number; attempt: number; status: string }[];
    const bad = failed.filter((t) => t.status === "failed");
    const first = bad[0]!;
    assert.deepEqual(bad.filter((t) => t.stage === first.stage && t.partition === first.partition).map((t) => t.attempt), [0, 1, 2, 3]);
  });

  test("a broadcast hash table bigger than a node's memory fails the query, saying to lower broadcastThreshold", async () => {
    const e = spark(db, "select count(*) from t a join t b on a.id = b.id", { settings: { broadcastThreshold: 1 << 30 }, cluster: { nodes: 1, cores: 1, memoryMB: 0.25, cpu: "model" } });
    assert.ok((e.state().ops as unknown as { name: string }[]).some((o) => o.name === "BroadcastHashJoin"), "the join is broadcast");
    await assert.rejects(e.run(), (err: Error) => {
      assert.ok(err instanceof OutOfMemoryError, `${err.name}: ${err.message}`);
      assert.match(err.message, /failed 4 times/);
      assert.match(err.message, /lower broadcastThreshold/);
      return true;
    });
  });

  test("shuffle files bigger than a node's disk fail the query with OutOfDiskError, after maxTaskFailures attempts", async () => {
    const e = spark(db, "select id, s, v from t order by v", { settings: { maxTaskFailures: 2 }, cluster: { nodes: 1, cores: 1, memoryMB: 64, diskMB: 0.05, cpu: "model" } });
    await assert.rejects(e.run(), (err: Error) => {
      assert.ok(err instanceof OutOfDiskError, `${err.name}: ${err.message}`);
      assert.match(err.message, /failed 2 times; most recent failure: .*ran out of disk/);
      return true;
    });
  });

  test("a sort bigger than a task's memory spills, and returns SQLite's rows", async () => {
    const sql = "select id, s, v from t order by v, id";
    const e = spark(db, sql, { cluster: { nodes: 2, cores: 2, memoryMB: 0.5, cpu: "model" } });
    const rows = await e.run();
    const want = sqlite.prepare(sql).all().map((r) => Object.values(r as object)) as Value[][];
    assertRows(rows, want, "spilled sort");
    const tasks = e.state().tasks as unknown as { spill_bytes: number }[];
    assert.ok(tasks.some((t) => t.spill_bytes > 0), "a task spilled");
  });
});

describe("Spark-style: Wave and Finish", () => {
  let db: Database;
  before(async () => {
    db = openDatabase();
    await db.loadSample();
  });
  const sql = () => SAMPLE.find((q) => q.id === "s1")?.sql ?? "";
  const cluster = { nodes: 2, cores: 2, cpu: "model" } as const;

  test("a wave runs at least one task, and at most one per core", async () => {
    const e = spark(db, sql(), { cluster });
    let waves = 0;
    while (!e.done) {
      const before = e.state().units as unknown as Unit[];
      const ran = await e.wave();
      const units = (e.state().units as unknown as Unit[]).slice(before.length);
      assert.equal(units.length, ran, "wave() returns the tasks it ran");
      if (e.done && ran === 0) break;
      assert.ok(ran >= 1, `wave ${waves} ran a task`);
      const cores = units.map((u) => `${u.node}:${u.core}`);
      assert.equal(new Set(cores).size, cores.length, `wave ${waves}: one task per core at most (${cores.join(", ")})`);
      assert.ok(ran <= cluster.nodes * cluster.cores);
      waves++;
    }
    assert.ok(waves > 1, `${waves} waves`);
  });

  test("Finish runs until every stage that was running is done", async () => {
    const e = spark(db, sql(), { cluster });
    let checked = 0;
    while (!e.done) {
      await e.step();
      const running = e.driver.stages.filter((s) => s.status.now === "running").map((s) => s.stage.id);
      if (!running.length) continue;
      await e.runConcurrent();
      for (const id of running) assert.equal(e.driver.stages[id]?.status.now, "done", `stage ${id} is done after Finish`);
      checked++;
    }
    assert.ok(checked >= 2, `Finish checked ${checked} times`);
  });
});
