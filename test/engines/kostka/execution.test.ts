import assert from "node:assert/strict";
import { before, describe, test } from "node:test";
import { OutOfDiskError, OutOfMemoryError, ExecutionError } from "../../../src/core/errors.ts";
import type { Database } from "../../../src/api/database.ts";
import type { QueryOptions } from "../../../src/api/query.ts";
import type { ExecEvent, Execution } from "../../../src/execution/engine.ts";
import { SimExecution } from "../../../src/engines/kostka/sim.ts";
import { tables } from "../../helpers/db.ts";

let db: Database;
before(async () => {
  db = await tables({
    f: { columns: [["k", "int"], ["v", "float"], ["s", "string"]], rows: Array.from({ length: 4000 }, (_, i) => [i % 500, i * 0.5, `s${i % 13}`]) },
    d: { columns: [["k", "int"], ["name", "string"]], rows: Array.from({ length: 500 }, (_, i) => [i, `name-${i}`]) },
    // small tables of one file each
    s1: { columns: [["k", "int"], ["a", "string"]], rows: Array.from({ length: 100 }, (_, i) => [i, `a${i}`]) },
    s2: { columns: [["k", "int"], ["b", "string"]], rows: Array.from({ length: 100 }, (_, i) => [i * 2, `b${i}`]) },
    s3: { columns: [["k", "int"], ["c", "string"]], rows: Array.from({ length: 100 }, (_, i) => [i * 3, `c${i}`]) },
    // one key holds most rows
    skew: { columns: [["k", "int"], ["v", "int"]], rows: Array.from({ length: 3000 }, (_, i) => [i % 3 ? 7 : i % 40, i]) },
  }, 250);
});

async function run(sql: string, o: QueryOptions = {}) {
  const q = db.compile(sql, o);
  const execution = kostka(q.start());
  const rows = await execution.run();
  return { rows, state: execution.state() as any, physical: execution.plan, execution };
}

/** kostka's execution, with its physical plan: the Database gives the shared Execution. */
function kostka(e: Execution): SimExecution {
  assert.ok(e instanceof SimExecution, "a kostka execution");
  return e;
}

/** Every message of the run, in order, with the step it belongs to. */
const messages = (ev: readonly ExecEvent[]) => ev.flatMap((e) => e.messages.map((m) => ({ ...m, step: e.seq, kind: e.kind })));

const W = (m: { to: string; from: string }, dir: "to" | "from") => Number(m[dir].slice(1));

const joinOpts = { cluster: { workers: 2, batchRows: 64, splitRows: 300 }, planner: { joinStrategy: "shuffle" as const, shufflePartitions: 4 } };
const joinSql = "select name, sum(v) from f join d on f.k = d.k group by name";
const key = (rows: readonly unknown[][]) => rows.map((r) => JSON.stringify(r)).sort().join("\n");

describe("execution: a coordinator and workers", () => {
  test("one split per data file, spread over every worker: there are no tasks", async () => {
    const { state } = await run("select s, count(*) from f group by s", { cluster: { workers: 3 } });
    const scan = state.pipelines[0];
    assert.equal(scan.splits.total, 16);
    const files = state.splits.filter((s: any) => s.pipeline === 0);
    assert.ok(files.every((s: any) => s.kind === "file" && s.status === "done" && s.attempt === 1));
    assert.equal(new Set(files.map((s: any) => s.worker)).size, 3);
    assert.equal(state.tasks, undefined);
    assert.ok(state.pipelines.every((p: any) => p.status === "done" && p.completions === 1));
  });

  test("independent file splits go to the least loaded workers, not all to the first", async () => {
    const { execution, physical } = await run("select count(*) from f join s1 on f.k = s1.k join s2 on f.k = s2.k join s3 on f.k = s3.k",
      { cluster: { workers: 8 }, planner: { joinStrategy: "broadcast" } });
    const builds = physical.pipelines.filter((p: any) => p.source.reading().kind === "files" && p.source.reading().files.length === 1).map((p: any) => p.index);
    assert.equal(builds.length, 3);
    const first = execution.events.filter((e) => e.kind === "assign" && builds.includes(e.refs.pipeline!));
    assert.equal(new Set(first.map((e) => e.node)).size, 3, first.map((e) => `P${e.refs.pipeline}→W${e.node}`).join(" "));
  });

  test("the protocol: AddSplits (k = 2 per worker and pipeline), SplitDone, ReleaseBuffer", async () => {
    const { execution, state, rows } = await run(joinSql, joinOpts);
    assert.equal(rows.length, 500);
    const ms = messages(execution.events);
    const pipelineOf = new Map<number, number>(state.splits.map((s: any) => [s.id, s.pipeline]));
    const queued = new Map<string, number>();
    const given = new Map<number, number>();
    for (const [i, m] of ms.entries()) {
      if (m.type === "AddSplits") {
        const p = Number(/P(\d+)/.exec(m.text)![1]);
        const ids = [...m.text.matchAll(/split (\d+)/g)].map((x) => Number(x[1]));
        const k = `${W(m, "to")}:${p}`;
        queued.set(k, (queued.get(k) ?? 0) + ids.length);
        assert.ok(queued.get(k)! <= 2, `${k} has ${queued.get(k)} queued`);
        for (const id of ids) given.set(id, i);
      }
      // a SplitDone shows twice: sent, in its process step, and handled, in its split_done step
      if (m.type === "SplitDone" && m.kind === "split_done") {
        const id = Number(/split (\d+)/.exec(m.text)![1]);
        const k = `${W(m, "from")}:${pipelineOf.get(id)}`;
        queued.set(k, queued.get(k)! - 1);
        assert.ok(given.get(id)! < i, `split ${id} was given out before it was done`);
      }
    }
    assert.ok([...queued.values()].every((n) => n === 0));
    // one SplitDone per split, and every buffer is released in the end
    assert.equal(ms.filter((m) => m.type === "SplitDone" && m.kind === "split_done").length, state.splits.length);
    assert.equal(ms.filter((m) => m.type === "SplitDone" && m.kind === "process").length, state.splits.length);
    assert.equal(ms.filter((m) => m.type === "ReleaseBuffer").length, state.buffers.length * 2);
    // only the coordinator sends commands; only workers send events
    assert.ok(ms.every((m) => (m.dir === "command") === (m.from === "coordinator")));
    for (const t of ["NoMoreSplits", "CreateTask", "TaskFinished", "SplitsCreated"]) assert.ok(!ms.some((m) => m.type === t), t);
    const kinds = new Set(execution.events.map((e) => e.kind));
    for (const k of ["start", "plan", "assign", "process", "split_done", "pipeline_done", "done"]) assert.ok(kinds.has(k as never), k);
  });

  test("outputs: the coordinator cuts splits from them; each partition has one owner", async () => {
    const { state } = await run(joinSql, joinOpts);
    const probe = state.pipelines.find((p: any) => p.label.includes("HashProbe"));
    const probeSide = state.buffers.find((b: any) => b.label === "probe side");
    const buildSide = state.buffers.find((b: any) => b.label === "build side");
    assert.equal(probeSide.partitioning, buildSide.partitioning, "co-partitioned");
    const splits = state.splits.filter((s: any) => s.pipeline === probe.index);
    assert.ok(splits.some((s: any) => s.parts > 1), "some partition is cut into several splits");
    // every output is read by exactly one split, of its own partition
    const read = splits.flatMap((s: any) => s.reads.map(([split, part]: number[]) => { assert.equal(part, s.partition); return `${split}:${part}`; }));
    assert.deepEqual([...read].sort(), probeSide.outputs.map((o: any) => `${o.split}:${o.partition}`).sort());
    assert.ok(splits.every((s: any) => s.rows <= 300 || s.reads.length === 1));
    for (let part = 0; part < 4; part++) {
      const workers = new Set(splits.filter((s: any) => s.partition === part).map((s: any) => s.worker));
      assert.deepEqual([...workers], [probe.owners[part]], `partition ${part} runs on its owner only`);
    }
    assert.ok(probeSide.outputs.every((o: any) => o.status === "released"));
    // an aggregate's merge reads whole partitions: one split each
    const merge = state.pipelines.find((p: any) => p.label.startsWith("MergeAggregate"));
    const ms = state.splits.filter((s: any) => s.pipeline === merge.index);
    assert.equal(new Set(ms.map((s: any) => s.partition)).size, ms.length);
  });

  test("an idle worker gets a partition that has not started on its owner", async () => {
    const o = { cluster: { workers: 2, cpu: "model" as const, io: "none" as const, batchRows: 64, splitRows: 100 }, planner: { shufflePartitions: 8, joinStrategy: "shuffle" as const } };
    const { state, rows, execution } = await run("select s.k, count(*) from skew s join d on s.k = d.k group by s.k", o);
    assert.equal(rows.length, 40);
    const moves = execution.events.filter((e) => /partition #\d+ has not started on its owner/.test(e.text));
    assert.ok(moves.length > 0, "a partition moved");
    for (const m of moves) {
      const part = Number(/partition #(\d+) has not/.exec(m.text)![1]);
      const ran = state.splits.filter((s: any) => s.pipeline === m.refs.pipeline && s.partition === part);
      const to = Number(/it moves to W(\d+)/.exec(m.text)![1]);
      assert.ok(ran.length && ran.every((s: any) => s.worker === to), "it ran where it moved");
      assert.equal(state.pipelines[m.refs.pipeline!].owners[part], to);
    }
  });

  test("a whole-buffer read is one split: merging sorted runs", async () => {
    const { state, rows } = await run("select k, v from f order by v desc limit 3", { cluster: { workers: 3 } });
    assert.deepEqual(rows.map((r) => r[1]), [1999.5, 1999, 1998.5]);
    const merge = state.pipelines.find((p: any) => p.label.startsWith("MergeTopN"));
    assert.ok(merge.whole);
    assert.equal(merge.splits.total, 1);
  });

  test("a limit has two phases: a gather buffer that says when it has enough", async () => {
    const { state, rows, execution } = await run("select k from f limit 5", { cluster: { workers: 3 } });
    assert.equal(rows.length, 5);
    const gather = state.buffers.find((b: any) => b.enough === 5);
    assert.ok(gather, "a gather buffer for the limit");
    assert.equal(gather.partitions, 1);
    assert.ok(execution.events.some((e) => e.kind === "enough"));
    assert.ok(state.pipelines[0].splits.skipped >= 5, JSON.stringify(state.pipelines[0].splits));
    const last = state.pipelines[gather.consumers[0]];
    assert.ok(last.whole && last.label.includes("Limit"));
  });

  test("step and wave", async () => {
    const q = db.compile("select s, count(*) from f group by s", { cluster: { workers: 4 } });
    const execution = q.start();
    assert.equal(execution.done, false);
    assert.ok((await execution.wave()) <= 4);
    assert.ok(await execution.step());
    assert.throws(() => execution.result(), ExecutionError);
    const before = (execution.state() as any).next;
    assert.ok(before && typeof before.kind === "string");
    while (await execution.step());
    assert.equal(execution.result().length, 13);
    assert.equal(await execution.step(), false);
  });

  test("history: the state after any step", async () => {
    const { execution, state: end } = await run(joinSql, joinOpts);
    const ev = execution.events;
    assert.deepEqual(ev.map((e) => e.seq), ev.map((_, i) => i));
    assert.equal(ev[ev.length - 1]!.kind, "done");
    assert.equal(ev.filter((e) => e.kind === "process").length, end.splits.length, "one process step per split");
    const s0 = execution.state(0) as any;
    assert.equal(s0.units_run, 0);
    assert.equal(s0.splits.length, 0);
    // an assign step queues its split on a worker
    const assign = ev.find((e) => e.kind === "assign" && e.refs.split !== undefined)!;
    const split = (execution.state(assign.seq) as any).splits.find((x: any) => x.id === assign.refs.split);
    assert.equal(split.status, "queued");
    assert.equal(split.worker, assign.node);
    assert.match(split.reason, /room|owns|moved/);
    // a processed split stays queued for the coordinator until its SplitDone arrives
    const proc = ev.find((e) => e.kind === "process")!;
    const sp = (execution.state(proc.seq) as any).splits.find((x: any) => x.id === proc.refs.split);
    assert.equal(sp.status, "queued");
    assert.ok(sp.ran);
    const doneStep = ev.find((e) => e.kind === "split_done" && e.refs.split === proc.refs.split)!;
    assert.equal((execution.state(doneStep.seq) as any).splits.find((x: any) => x.id === proc.refs.split).status, "done");
    // outputs appear when the coordinator hears of them; buffers are sealed, then released
    const outputs = (st: any) => st.buffers.reduce((n: number, b: any) => n + b.outputs.length, 0);
    const first = ev.find((e) => e.kind === "split_done" && outputs(execution.state(e.seq)) > 0)!;
    assert.equal(outputs(execution.state(first.seq - 1)), 0);
    const sealed = ev.find((e) => e.kind === "pipeline_done" && e.refs.buffer !== undefined)!;
    assert.match((execution.state(sealed.seq) as any).buffers[sealed.refs.buffer!].status, /sealed|released/);
    assert.ok(end.buffers.every((b: any) => b.status === "released"));
    assert.ok(end.nodes.every((w: any) => w.cache.length === 0 && w.queued.length === 0));
    // every state is plain data
    for (let i = 0; i < ev.length; i += 7) structuredClone(execution.state(i));
  });

  test("with the cost model, simulated time is deterministic and more workers finish sooner", async () => {
    const sql = "select s, sum(v) from f group by s";
    const a = await run(sql, { cluster: { workers: 1, cpu: "model" } }), b = await run(sql, { cluster: { workers: 1, cpu: "model" } });
    assert.equal(a.execution.elapsedNs, b.execution.elapsedNs);
    assert.deepEqual(a.execution.events.map((e) => e.text), b.execution.events.map((e) => e.text));
    const c = await run(sql, { cluster: { workers: 4, cpu: "model" } });
    assert.ok(c.execution.elapsedNs < a.execution.elapsedNs * 0.6, `${c.execution.elapsedNs} vs ${a.execution.elapsedNs}`);
  });

  test("IO profiles change IO time only", async () => {
    const sql = "select count(*) from f where v > 10";
    const none = await run(sql, { cluster: { io: "none", cpu: "model" } }), slow = await run(sql, { cluster: { io: "slow", cpu: "model" } });
    assert.equal(none.state.nodes[0].time_ms.storage, 0);
    assert.ok(slow.state.nodes[0].time_ms.storage > 0);
    assert.equal(none.state.nodes[0].time_ms.cpu, slow.state.nodes[0].time_ms.cpu);
  });

  test("measured CPU: every operator's own time, adding up to the workers' CPU time", async () => {
    const sql = "select name, sum(v) from f join d on f.k = d.k where v > 3 group by name order by name";
    const m = await run(sql, { cluster: { workers: 2, io: "none" } });
    assert.equal(m.state.cluster.cpu, "measured");
    const model = await run(sql, { cluster: { workers: 2, io: "none", cpu: "model" } });
    assert.deepEqual(m.rows, model.rows);
    const workersCpu = m.state.nodes.reduce((a: number, w: any) => a + w.time_ms.cpu, 0);
    const opsCpu = m.state.pipelines.flatMap((p: any) => p.ops).reduce((a: number, o: any) => a + o.metrics.time_ms.cpu, 0);
    assert.ok(workersCpu > 0, "some CPU time was measured");
    assert.ok(Math.abs(workersCpu - opsCpu) < 1e-6 * Math.max(1, workersCpu), `${workersCpu} vs ${opsCpu}`);
    const scan = m.state.pipelines.flatMap((p: any) => p.ops).find((o: any) => o.label.startsWith("Scan(f"));
    assert.ok(scan.metrics.time_ms.cpu > 0, "decoding counts as the scan's CPU time");
  });

  test("memory: spilling when short, released at the end, out of memory when a table can't fit", async () => {
    const roomy = await run(joinSql, { cluster: { workers: 2 }, planner: { joinStrategy: "shuffle" } });
    assert.ok(roomy.state.nodes.every((w: any) => w.memory.used === 0 && w.disk.used === 0), "everything released");
    const short = await run(joinSql, { cluster: { workers: 2, memoryMB: 0.03, batchRows: 64 }, planner: { joinStrategy: "shuffle", shufflePartitions: 8 } });
    assert.ok(short.state.nodes.some((w: any) => w.spills > 0), "spilled");
    assert.deepEqual([...short.rows].sort(), [...roomy.rows].sort());
    await assert.rejects(run(joinSql, { cluster: { workers: 2, memoryMB: 0.01, batchRows: 64 }, planner: { joinStrategy: "shuffle", shufflePartitions: 8 } }), OutOfMemoryError);
  });

  test("disk: a spill that doesn't fit on the worker's disk fails the query", async () => {
    const o = { workers: 2, memoryMB: 0.03, batchRows: 64 };
    const short = await run(joinSql, { cluster: { ...o, diskMB: 64 }, planner: { joinStrategy: "shuffle", shufflePartitions: 8 } });
    const peak = Math.max(...short.state.nodes.map((w: any) => w.disk.peak));
    assert.ok(peak > 0 && short.state.nodes.every((w: any) => w.disk.capacity === 64 * 1024 * 1024));
    await assert.rejects(run(joinSql, { cluster: { ...o, diskMB: peak / 2 / 1024 / 1024 }, planner: { joinStrategy: "shuffle", shufflePartitions: 8 } }),
      (e: Error) => e instanceof OutOfDiskError && /ran out of disk/.test(e.message));
  });

  test("shuffle partitions: auto is 2 per worker", () => {
    const sql = "select k, count(*) from f group by k";
    for (const workers of [1, 3]) {
      const physical = kostka(db.compile(sql, { cluster: { workers } }).start()).plan;
      assert.deepEqual(physical.buffers.filter((b) => b.mode === "shuffle").map((b) => b.n), [2 * workers]);
    }
    const physical = kostka(db.compile(sql, { cluster: { workers: 3 }, planner: { shufflePartitions: 5 } }).start()).plan;
    assert.deepEqual(physical.buffers.filter((b) => b.mode === "shuffle").map((b) => b.n), [5]);
  });

  test("a failed query keeps failing", async () => {
    const q = db.compile("select k + 9007199254740990 + k from f");
    const execution = q.start();
    await assert.rejects(execution.run(), ExecutionError);
    await assert.rejects(execution.step(), /failed/);
    const st = execution.state() as any;
    assert.ok(st.failed.error.includes("overflow"));
    assert.equal(execution.events[execution.events.length - 1]!.kind, "failed");
    assert.ok(messages(execution.events).some((m) => m.type === "SplitFailed"));
  });

  test("a join that multiplies rows emits them in batches of at most batchRows", async () => {
    // without filter_pushdown the join condition stays in a Filter above a cross join
    const rules = ["file_pruning", "join_reorder", "projection_pushdown", "topn"];
    const { state, rows } = await run("select count(*) from f, d where f.k = d.k", { rules, cluster: { workers: 2, batchRows: 1000 } });
    assert.deepEqual(rows, [[4000]]);
    const probe = state.pipelines.flatMap((p: any) => p.ops).find((o: any) => o.label.startsWith("HashProbe(cross"));
    assert.equal(probe.metrics.rows_out, 4000 * 500);
    assert.ok(probe.metrics.batches_out >= (4000 * 500) / 1000, `${probe.metrics.batches_out} batches`);
  });

  test("the coordinator refuses a plan estimated to produce more than a billion rows", async () => {
    const rules = ["file_pruning", "join_reorder", "projection_pushdown", "topn"];
    const sql = "select count(*) from f, d, skew where f.k = d.k and d.k = skew.k";
    const execution = db.compile(sql, { rules }).start();
    await assert.rejects(execution.run(), /Join cross is estimated to produce 6,000,000,000 rows.*about 25 minutes.*filter_pushdown/);
    assert.deepEqual(execution.events.map((e) => e.kind), ["start", "failed"]);
    assert.equal((execution.state() as any).units_run, 0);
    // with the rule, the same query runs
    assert.deepEqual((await run(sql)).rows, [[(await run("select count(*) from f join d on f.k = d.k join skew on d.k = skew.k")).rows[0]![0]]]);
  });

  test("runtime filters: build splits report keys, the coordinator skips probe files", async () => {
    const { state, rows, execution } = await run("select count(*) from f join d on f.k = d.k where d.name = 'name-3'", { cluster: { workers: 2 } });
    assert.deepEqual(rows, [[8]]);
    const scan = state.pipelines.find((p: any) => p.label.startsWith("Scan(f"));
    assert.ok(scan.splits.total < 16, String(scan.splits.total));
    assert.equal(scan.skipped_files.length, 16 - scan.splits.total);
    assert.equal(state.filters[0].keys, 1);
    assert.ok(messages(execution.events).some((m) => m.type === "SplitDone" && m.text.includes("runtime filter")));
  });
});

describe("execution: fault tolerance", () => {
  const queries: [string, QueryOptions][] = [
    ["select name, sum(v), count(*) from f join d on f.k = d.k group by name", { cluster: { workers: 3, cpu: "model", batchRows: 64, splitRows: 300 }, planner: { joinStrategy: "shuffle", shufflePartitions: 4 } }],
    ["select name, sum(v) from f join d on f.k = d.k group by name order by 2 desc limit 7", { cluster: { workers: 2, cpu: "model" }, planner: { joinStrategy: "broadcast" } }],
    ["select count(*) from f", { cluster: { workers: 2, cpu: "model" } }],
  ];

  for (const [sql, o] of queries) {
    test(`a worker stopped at any step: the same result (${sql.slice(0, 40)}…)`, async () => {
      const want = await db.compile(sql, o).start().run();
      const steps = (await run(sql, o)).execution.events.length;
      let reruns = 0;
      for (let at = 0; at < steps; at += 3) {
        const e = db.compile(sql, o).start();
        for (let i = 0; i < at && !e.done; i++) await e.step();
        if (e.done) break;
        e.stopNode(at % 2);
        const got = await e.run();
        assert.equal(key(got), key(want), `stopped W${at % 2} after ${at} steps`);
        const st = e.state() as any;
        assert.equal(st.nodes[at % 2].status, "gone");
        assert.ok(e.events.some((x) => x.kind === "worker_gone"));
        // nothing ran on the gone worker after it was gone
        const gone = e.events.find((x) => x.kind === "worker_gone")!.seq;
        assert.ok(!st.units.some((u: any) => u.node === at % 2 && u.seq > gone));
        if (st.splits.some((s: any) => s.attempt > 1)) reruns++;
      }
      assert.ok(reruns > 0, "some split ran again");
    });
  }

  test("lost outputs are made again from their lineage", async () => {
    const [sql, o] = queries[0]!;
    const e = db.compile(sql, o).start();
    // stop a worker once the join's build and probe sides are done
    while (!e.events.some((x) => x.kind === "pipeline_done" && x.refs.pipeline === 1)) await e.step();
    const before = e.state() as any;
    const held = before.buffers.flatMap((b: any) => b.outputs.filter((x: any) => x.worker === 0 && x.status === "available"));
    assert.ok(held.length, "W0 holds outputs");
    e.stopNode(0);
    const after = e.state() as any;
    const lost = after.buffers.flatMap((b: any) => b.outputs.filter((x: any) => x.status === "lost"));
    assert.ok(lost.length > 0 && lost.every((x: any) => x.worker === 0));
    // the owners of W0's partitions are now workers that are up
    for (const p of after.pipelines) for (const w of Object.values(p.owners)) assert.notEqual(w, 0);
    await e.run();
    const end = e.state() as any;
    assert.ok(end.pipelines.some((p: any) => p.completions > 1), "a done pipeline ran again");
    assert.ok(end.splits.some((s: any) => s.attempt > 1));
  });

  test("two workers stopped in turn; every worker stopped fails the query", async () => {
    const [sql, o] = queries[0]!;
    const want = await db.compile(sql, o).start().run();
    const e = db.compile(sql, o).start();
    for (let i = 0; i < 30; i++) await e.step();
    e.stopNode(1);
    assert.throws(() => e.stopNode(1), /gone already/);
    for (let i = 0; i < 30; i++) await e.step();
    e.stopNode(0);
    assert.equal(key(await e.run()), key(want));
    assert.throws(() => e.stopNode(2), /over/);

    const f = db.compile(sql, o).start();
    for (let i = 0; i < 10; i++) await f.step();
    for (const w of [0, 1, 2]) f.stopNode(w);
    await assert.rejects(f.run(), /every worker is gone/);
    assert.equal(f.events[f.events.length - 1]!.kind, "failed");
  });
});
