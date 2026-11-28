// The Spark-style engine's planner and its cut into jobs and stages (ADR 0002):
// - criterion 2: on the sample with the default settings, "revenue per city" plans a
//   SortMergeJoin with a hash Exchange on each side and "orders from one city" a
//   BroadcastHashJoin; SSB Q4.1 at scale 0.1 has both;
// - the operators each plan node becomes: a Sort, a TopN, a Limit and a join without
//   keys, and no Exchange where the input is in one partition already;
// - criterion 3: one stage per Exchange plus one result stage per job, one job per
//   BroadcastExchange plus one, and each stage's task count (SSB Q2.1, TPC-DS q65).
import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { openDatabase } from "../../../src/node.ts";
import type { Database } from "../../../src/api/database.ts";
import type { QueryOptions } from "../../../src/api/query.ts";
import { SparkExecution } from "../../../src/engines/spark/sim.ts";
import { BroadcastExchange, ShuffleExchange } from "../../../src/engines/spark/physical/operators/exchange.ts";
import { BroadcastHashJoinExec, SortMergeJoinExec } from "../../../src/engines/spark/physical/operators/join.ts";
import { SortExec } from "../../../src/engines/spark/physical/operators/sort.ts";
import { walk, type SparkOp } from "../../../src/engines/spark/physical/plan.ts";
import { sparkSettings } from "../../../src/engines/spark/engine.ts";
import { QUERIES as SSB } from "../../../src/loaders/ssb/queries.ts";
import { QUERIES as TPCDS } from "../../../src/loaders/tpcds/queries.ts";
import { QUERIES as SAMPLE } from "../../../src/loaders/sample/queries.ts";

const sql = (list: readonly { id: string; sql: string }[], id: string) => {
  const q = list.find((x) => x.id === id);
  assert.ok(q, `there is a query ${id}`);
  return q.sql;
};

const REVENUE_PER_CITY = sql(SAMPLE, "s1");
const ORDERS_FROM_ONE_CITY = sql(SAMPLE, "s3");

function spark(db: Database, sql: string, o: QueryOptions = {}): SparkExecution {
  const e = db.compile(sql, { ...o, engine: "spark" }).start();
  assert.ok(e instanceof SparkExecution, "a Spark-style execution");
  return e;
}

const ops = (e: SparkExecution): SparkOp[] => [...walk(e.plan.root)];

/** A SortMergeJoin whose two inputs are each a Sort over a hash Exchange. */
const shuffled = (j: SortMergeJoinExec) =>
  j.children().every((c) => c instanceof SortExec && c.child instanceof ShuffleExchange && c.child.target.kind === "hash");

describe("the Spark-style planner: broadcast or shuffle", () => {
  test("the sample: revenue per city shuffles both sides; orders from one city broadcasts", async () => {
    const db = openDatabase();
    await db.loadSample();
    const a = ops(spark(db, REVENUE_PER_CITY));
    const smj = a.filter((o): o is SortMergeJoinExec => o instanceof SortMergeJoinExec);
    assert.equal(smj.length, 1, "one SortMergeJoin");
    assert.ok(shuffled(smj[0]!), "a hash Exchange and a Sort on each side");
    assert.ok(!a.some((o) => o instanceof BroadcastHashJoinExec));
    const b = ops(spark(db, ORDERS_FROM_ONE_CITY));
    assert.equal(b.filter((o) => o instanceof BroadcastHashJoinExec).length, 1);
    assert.ok(!b.some((o) => o instanceof SortMergeJoinExec));
    // the threshold decides: raised, revenue per city broadcasts too; -1, nothing does
    assert.ok(ops(spark(db, REVENUE_PER_CITY, { settings: { broadcastThreshold: 1 << 20 } })).some((o) => o instanceof BroadcastHashJoinExec));
    assert.ok(!ops(spark(db, ORDERS_FROM_ONE_CITY, { settings: { broadcastThreshold: -1 } })).some((o) => o instanceof BroadcastHashJoinExec));
  });

  test("SSB Q4.1 at scale 0.1 has both kinds of join", async () => {
    const db = openDatabase();
    await db.loadSsb(0.1);
    const o = ops(spark(db, sql(SSB, "ssb4.1")));
    assert.ok(o.some((x) => x instanceof BroadcastHashJoinExec), "a BroadcastHashJoin");
    const smj = o.filter((x): x is SortMergeJoinExec => x instanceof SortMergeJoinExec);
    assert.ok(smj.length > 0 && smj.every(shuffled), "a SortMergeJoin with a hash Exchange on each side");
  });

  test("settings are checked", async () => {
    assert.deepEqual(sparkSettings({}), { broadcastThreshold: 32768, shufflePartitions: "auto", maxTaskFailures: 4 });
    assert.throws(() => sparkSettings({ joinStrategy: "shuffle" }), /no setting "joinStrategy"/);
    assert.throws(() => sparkSettings({ broadcastThreshold: -2 }), /broadcastThreshold must be/);
    assert.throws(() => sparkSettings({ maxTaskFailures: 0 }), /maxTaskFailures must be/);
  });
});

describe("the Spark-style planner: operators", () => {
  let db: Database;
  before(async () => {
    db = openDatabase();
    await db.loadSample();
  });

  /** The plan's operators by name, a line each, indented by depth; partial operators marked. */
  const shape = (text: string) => {
    const lines: string[] = [];
    const visit = (op: SparkOp, depth: number) => {
      const arg = op instanceof ShuffleExchange ? ` ${op.target.kind}` : /partial/.test(op.args()) ? " partial" : "";
      lines.push(`${"  ".repeat(depth)}${op.name()}${arg}`);
      for (const c of op.children()) visit(c, depth + 1);
    };
    visit(spark(db, text).plan.root, 0);
    return lines;
  };

  test("Sort: an Exchange to one partition, then a Sort", () => {
    assert.deepEqual(shape("select id from orders order by amount").slice(0, 3), ["Project", "  Sort", "    Exchange single"]);
  });

  test("TopN: TakeOrderedAndProject in each task, then once more in one partition", () => {
    assert.deepEqual(shape("select * from orders order by amount limit 3").slice(0, 3), ["TakeOrderedAndProject", "  Exchange single", "    TakeOrderedAndProject partial"]);
  });

  test("Limit: LocalLimit in each task, an Exchange to one partition, then GlobalLimit", () => {
    assert.deepEqual(shape("select * from orders limit 3").slice(0, 3), ["GlobalLimit", "  Exchange single", "    LocalLimit"]);
  });

  test("a join without keys: a BroadcastNestedLoopJoin, its right side broadcast", () => {
    assert.deepEqual(shape("select o.id, u.id from orders o join users u on o.amount > u.id"), ["Project", "  BroadcastNestedLoopJoin", "    FileScan", "    BroadcastExchange", "      FileScan"]);
  });

  test("no Exchange where the input is in one partition already", () => {
    // Above a Sort, the rows are in one partition: the Limit is a GlobalLimit alone.
    assert.deepEqual(shape("select * from (select * from orders order by amount) x limit 3").slice(0, 4), ["GlobalLimit", "  Project", "    Sort", "      Exchange single"]);
    // A final aggregate without keys is one partition too.
    assert.deepEqual(shape("select count(*) from orders limit 1"), ["GlobalLimit", "  Project", "    HashAggregate", "      Exchange single", "        HashAggregate partial", "          FileScan"]);
  });
});

describe("the Spark-style planner: jobs, stages and tasks", () => {
  let ssb: Database, tpcds: Database;
  before(async () => {
    ssb = openDatabase();
    await ssb.loadSsb(0.01);
    tpcds = openDatabase();
    await tpcds.loadTpcds(0.01);
  });

  for (const [name, db, text] of [["SSB Q2.1", () => ssb, () => sql(SSB, "ssb2.1")], ["TPC-DS q65", () => tpcds, () => sql(TPCDS, "q65")]] as const) {
    for (const cluster of [{ nodes: 1, cores: 1 }, { nodes: 3, cores: 2 }]) {
      test(`${name} on ${cluster.nodes} node(s) × ${cluster.cores} core(s)`, () => {
        const e = spark(db(), text(), { cluster });
        const all = ops(e);
        const shuffles = all.filter((o): o is ShuffleExchange => o instanceof ShuffleExchange);
        const broadcasts = all.filter((o) => o instanceof BroadcastExchange);
        const { jobs, stages } = e.stages;
        assert.equal(jobs.length, broadcasts.length + 1, "one job per BroadcastExchange plus one");
        assert.equal(stages.length, shuffles.length + jobs.length, "one stage per Exchange plus one result stage per job");
        assert.equal(stages.filter((s) => s.kind === "result").length, jobs.length);
        const n = cluster.nodes * cluster.cores * 2;
        assert.equal(e.plan.settings.shufflePartitions, n, "auto: 2 per core in the cluster");
        for (const s of stages) {
          const reads = s.reads.map((r) => shuffles.find((x) => x.shuffle === r)!);
          if (s.scans.length) {
            for (const scan of s.scans) assert.equal(s.partitions, Math.max(1, scan.scan.files.length), `stage ${s.id}: one task per file`);
          } else if (reads.some((x) => x.target.kind === "hash")) assert.equal(s.partitions, n, `stage ${s.id} reads a hash Exchange`);
          else assert.equal(s.partitions, 1, `stage ${s.id} reads a SinglePartition`);
        }
        // a stage's parents, and the jobs it needs, come before it
        for (const s of stages) for (const p of s.parents) assert.ok(p < s.id);
        for (const j of jobs) for (const x of j.needs) assert.ok(x < j.id);
      });
    }
  }
});
