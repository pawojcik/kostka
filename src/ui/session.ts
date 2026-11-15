// What the workbench asks of the engine. A Session runs next to the Database (in the
// browser, in a Web Worker; in tests, in Node) and answers with plain JSON-friendly
// objects: tables, loaders, and queries that are compiled once and then stepped.
// A query's history is its execution's steps (the coordinator's and the workers'
// messages, grouped): the page can show the state right after any of them, and Step
// moves one step on (running the simulation only when it has seen all steps so far).

import { KostkaError, SqlError } from "../core/errors.ts";
import { COLUMN_TYPES, type ColumnType, type Value } from "../core/types.ts";
import { allFunctions } from "../core/functions/registry.ts";
import { allAggregates } from "../core/functions/aggregates.ts";
import { parse } from "../sql/parser.ts";
import { analyze } from "../sql/analyzer.ts";
import type { PlanNode } from "../sql/plan.ts";
import { Estimator } from "../sql/optimizer/cardinality.ts";
import { ALL_RULES, type CompiledQuery, type QueryOptions } from "../engine/query.ts";
import type { Database } from "../engine/database.ts";
import type { PhysicalPlan } from "../physical/pipeline.ts";
import type { PhysicalOperator, Sink } from "../physical/operator.ts";
import type { EventKind, Execution } from "../execution/runtime.ts";
import { toJson } from "../core/json.ts";
import { CPU_MODES, IO_PROFILES, MEMORY_OPTIONS_MB, DISK_OPTIONS_MB, BATCH_SIZES, WORKER_COUNTS, DEFAULT_CLUSTER } from "../execution/cluster.ts";
import { GENERATOR_KINDS, type Generator } from "../loaders/generators.ts";
import type { CsvColumn, CsvOptions } from "../loaders/csv.ts";
import { QUERIES } from "../loaders/tpcds/queries.ts";
import { QUERIES as SSB_QUERIES } from "../loaders/ssb/queries.ts";
import { QUERIES as TAXI_QUERIES } from "../loaders/taxi/queries.ts";
import { FILES as TAXI_FILES } from "../loaders/taxi/load.ts";
import { SCALES } from "../loaders/tpcds/generate.ts";
import type { WriteMode } from "../storage/writer.ts";

export const MAX_ROWS_SHOWN = 50;
export const MAX_RESULT_ROWS = 500;

export type Progress = (event: { phase?: string; done?: number; total?: number; log?: string }) => void;

export interface TreeNode {
  id: number;
  kind: string;
  detail: string;
  columns: { name: string; type: string }[];
  estimate: number;
  children: TreeNode[];
}

/** Plan trees as JSON; `ids` numbers the nodes so operators can point at them. */
export function planTree(root: PlanNode, ids = new Map<PlanNode, number>()): TreeNode {
  const est = new Estimator();
  const walk = (n: PlanNode): TreeNode => {
    if (!ids.has(n)) ids.set(n, ids.size);
    let estimate = -1;
    try {
      estimate = Math.round(est.rows(n));
    } catch {
      // no estimate for this node
    }
    return {
      id: ids.get(n)!, kind: n.kind, detail: n.detail(), estimate,
      columns: n.schema.map((f) => ({ name: f.label(), type: f.type })), children: n.children().map(walk),
    };
  };
  return walk(root);
}

interface OpenQuery {
  id: string;
  compiled: CompiledQuery;
  physical: PhysicalPlan;
  execution: Execution;
  nodeIds: Map<PlanNode, number>;
  error: string | null;
}

export interface CompileOptions extends QueryOptions {}

export class Session {
  readonly db: Database;
  readonly storage: string;
  private readonly queries = new Map<string, OpenQuery>();

  constructor(db: Database, storage = "memory") {
    this.db = db;
    this.storage = storage;
  }

  // --- reference data ------------------------------------------------------------

  info() {
    return {
      storage: this.storage,
      rules: ALL_RULES.map((r) => ({ name: r.name, doc: r.doc, cls: r.constructor.name })),
      cluster: {
        defaults: DEFAULT_CLUSTER, workers: WORKER_COUNTS, memory: MEMORY_OPTIONS_MB, disk: DISK_OPTIONS_MB, batches: BATCH_SIZES,
        io: Object.values(IO_PROFILES).map((p) => ({ name: p.name, label: p.label })),
        cpu: CPU_MODES,
      },
      types: COLUMN_TYPES,
      generatorKinds: GENERATOR_KINDS,
      tpcdsScales: SCALES,
      examples: [
        ...TAXI_QUERIES.map(({ id, title, about, sql }) => ({ id, title, about, sql, needs: ["trips.pickup_time", "zones.zone_id"], dataset: "NYC taxi" })),
        ...SSB_QUERIES.map(({ id, title, about, sql }) => ({ id, title, about, sql, needs: ["lineorder.lo_orderdate", "customer.c_region"], dataset: "SSB" })),
        ...QUERIES.map(({ id, title, about, sql }) => ({ id, title, about, sql, needs: ["store_sales.ss_sold_date_sk", "customer.c_customer_sk"], dataset: "TPC-DS" })),
      ],
      functions: [
        ...allFunctions().filter((f) => !f.operator).map((f) => ({ name: f.name, doc: f.doc, signature: f.signature ?? f.name, kind: "function" })),
        ...allAggregates().map((a) => ({ name: a.name, doc: a.doc, signature: a.signature ?? a.name, kind: "aggregate" })),
      ],
    };
  }

  /** Tables and their columns, for autocomplete. */
  schema(): Record<string, { name: string; type: ColumnType }[]> {
    return Object.fromEntries(this.db.listTables().map((t) => [t, this.db.describe(t).columns.map((c) => ({ name: c.name, type: c.type }))]));
  }

  // --- tables --------------------------------------------------------------------

  tables() {
    return this.db.listTables().map((name) => {
      const meta = this.db.describe(name);
      const files = this.db.files(name);
      return {
        name, version: meta.version, columns: meta.columns.length,
        rows: files.reduce((s, f) => s + f.rowCount, 0), files: files.length, bytes: files.reduce((s, f) => s + f.sizeBytes, 0),
      };
    });
  }

  async table(name: string, version?: number) {
    const meta = this.db.describe(name);
    const v = version ?? meta.version;
    const snap = meta.snapshot(v)!;
    const files = this.db.files(name, v);
    const { rows } = await this.db.query(`select * from ${name} for version as of ${v} limit ${MAX_ROWS_SHOWN}`);
    return {
      name, version: v, current: meta.version, location: meta.location,
      columns: snap.columns.map((c) => ({ name: c.name, type: c.type })),
      rows, total: files.reduce((s, f) => s + f.rowCount, 0),
      history: this.db.history(name).map((h) => ({ version: h.version, createdUs: h.createdUs, operation: h.operation, rows: h.rows, detail: h.detail, files: h.files, columns: h.columns.map((c) => `${c.name} ${c.type}`) })),
      files: files.map((f) => ({
        path: f.path, rows: f.rowCount, bytes: f.sizeBytes, position: f.position,
        stats: [...f.stats].map(([col, s]) => ({ column: col, min: s.min, max: s.max, nulls: s.nullCount, distinct: s.distinct, bloom: !!s.bloom, sketch: s.sketch ? Math.round(s.sketch.estimate()) : null })),
      })),
    };
  }

  async createTable(name: string, columns: [string, string][]) {
    await this.db.createTable(name, columns);
    return this.tables();
  }

  async dropTable(name: string) {
    await this.db.dropTable(name);
    return this.tables();
  }

  revert(name: string, version: number) {
    this.db.revert(name, version);
    return this.tables();
  }

  settings() {
    return { fileRows: this.db.fileRows };
  }

  setFileRows(n: number) {
    this.db.fileRows = n;
    return this.settings();
  }

  csvInfer(text: string, o: CsvOptions = {}) {
    return this.db.inferCsv(text, o);
  }

  async csvImport(text: string, table: string, mode: WriteMode, columns: CsvColumn[], o: CsvOptions = {}, detail = "CSV import") {
    const rows = await this.db.importCsv(text, table, mode, columns, o, detail);
    return { rows, tables: this.tables() };
  }

  defaultGenerators(name: string) {
    return this.db.defaultGenerators(name);
  }

  async generate(name: string, n: number, o: { sortBy?: string | null; generators?: Record<string, Generator>; seed?: number; mode?: WriteMode; columns?: [string, string][] } = {}, progress?: Progress) {
    const t0 = performance.now();
    progress?.({ phase: `Generating ${n.toLocaleString("en-US")} rows into ${name}`, done: 0, total: 1 });
    await this.db.generate(name, n, o);
    progress?.({ phase: "Done", done: 1, total: 1, log: `${name}: ${n.toLocaleString("en-US")} rows (${Math.round(performance.now() - t0)} ms)` });
    return this.tables();
  }

  async loadSample(progress?: Progress) {
    progress?.({ phase: "Generating users and orders", done: 0, total: 1 });
    await this.db.loadSample();
    progress?.({ phase: "Done", done: 1, total: 1, log: "users: 2,000 rows; orders: 20,000 rows" });
    return this.tables();
  }

  loadTpcds(scale: number, progress?: Progress) {
    return this.loadBenchmark("TPC-DS", (o) => this.db.loadTpcds(scale, o), scale, progress);
  }

  loadSsb(scale: number, progress?: Progress) {
    return this.loadBenchmark("SSB", (o) => this.db.loadSsb(scale, o), scale, progress);
  }

  /** Loads the NYC taxi sample. Without `files`, fetches them next to the page. */
  loadTaxi(files: Record<string, Uint8Array> | null = null, progress?: Progress) {
    return this.loadBenchmark("NYC taxi", async (o) => {
      if (!files) {
        files = {};
        for (const [table, path] of Object.entries(TAXI_FILES)) {
          o.log(`fetching ${path}`);
          const res = await fetch(new URL(path, globalThis.location?.href));
          if (!res.ok) throw new Error(`could not fetch ${path}: HTTP ${res.status}`);
          files[table] = new Uint8Array(await res.arrayBuffer());
        }
      }
      return this.db.loadTaxi(files, o);
    }, null, progress);
  }

  private async loadBenchmark(name: string, load: (o: { report: (table: string, done: number, total: number) => void; log: (text: string) => void }) => Promise<Record<string, number>>, scale: number | null, progress?: Progress) {
    const t0 = performance.now();
    let last = 0;
    const counts = await load({
      report: (table, done, total) => {
        const now = performance.now();
        if (done !== 0 && done < total && now - last < 150) return;
        last = now;
        progress?.({ phase: `${table} (${total.toLocaleString("en-US")} rows)`, done, total });
      },
      log: (text) => progress?.({ log: text }),
    });
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    progress?.({ phase: "Done", done: 1, total: 1, log: `${name}${scale === null ? "" : ` scale ${scale}`}: ${total.toLocaleString("en-US")} rows in ${((performance.now() - t0) / 1000).toFixed(1)} s` });
    return this.tables();
  }

  // --- queries -------------------------------------------------------------------

  /** Parse and analysis errors with their positions, for the editor's squiggles. */
  check(sql: string): { from: number; to: number; message: string }[] {
    if (!sql.trim()) return [];
    try {
      analyze(parse(sql), this.db.catalog);
      return [];
    } catch (e) {
      if (e instanceof SqlError) {
        const [from, to] = e.span ?? [0, sql.length];
        return [{ from, to: Math.max(to, from + 1), message: e.message }];
      }
      if (e instanceof KostkaError) return [{ from: 0, to: sql.length, message: e.message }];
      throw e;
    }
  }

  compile(qid: string, sql: string, o: CompileOptions = {}) {
    this.queries.delete(qid);
    const compiled = this.db.compile(sql, o);
    const { physical, execution } = compiled.start();
    const nodeIds = new Map<PlanNode, number>();
    const q: OpenQuery = { id: qid, compiled, physical, execution, nodeIds, error: null };
    const optimized = planTree(compiled.optimized, nodeIds);
    this.queries.set(qid, q);
    const t0 = compiled.phases[0]?.start ?? 0;
    const plans = {
      logical: planTree(compiled.logical),
      optimized,
      steps: compiled.trace.map((t) => ({ rule: t.rule, iteration: t.iteration, tree: planTree(t.plan) })),
      // Real ms from the start of parsing: the phases, and each optimizer pass.
      phases: compiled.phases.map((p) => ({ phase: p.phase, start: p.start - t0, end: p.end - t0 })),
      timings: compiled.timings.map((t) => ({ rule: t.rule, iteration: t.iteration, start: t.start - t0, end: t.end - t0, changed: t.changed })),
      columns: compiled.columns.map((c) => ({ name: c.name, type: c.type })),
      explain: physical.explain(),
      cluster: compiled.cluster,
    };
    return { plans, ...this.answer(q, execution.events.length - 1, 0) };
  }

  private get(qid: string): OpenQuery {
    const q = this.queries.get(qid);
    if (!q) throw new KostkaError("this query is not compiled; compile it first");
    return q;
  }

  /** The state at step `at`, and the steps the page does not have yet (it has the
   * first `known`). */
  private answer(q: OpenQuery, at: number, known: number) {
    const events = q.execution.events;
    return { state: this.snapshot(q, at), at, total: events.length, events: events.slice(known) };
  }

  /** Steps until `want` says the steps include what it waits for, the query is
   * done, or it fails. */
  private async until(q: OpenQuery, want: () => boolean, onProgress?: () => void): Promise<void> {
    try {
      while (!want() && !q.execution.done) {
        await q.execution.step();
        onProgress?.();
      }
    } catch (e) {
      q.error = `${(e as Error).name}: ${(e as Error).message}`;
    }
  }

  private last(q: OpenQuery): number {
    return q.execution.events.length - 1;
  }

  /** The next step after `from` (default: the last one). */
  async step(qid: string, from?: number, known = 0) {
    const q = this.get(qid);
    const at = (from ?? this.last(q)) + 1;
    await this.until(q, () => this.last(q) >= at);
    return this.answer(q, Math.min(at, this.last(q)), known);
  }

  /** The next step of one of these kinds after `from` (default: a split processed). */
  async next(qid: string, from?: number, known = 0, kinds: EventKind[] = ["process"]) {
    const q = this.get(qid);
    const start = (from ?? this.last(q)) + 1;
    const find = () => q.execution.events.findIndex((e, i) => i >= start && kinds.includes(e.kind));
    await this.until(q, () => find() >= 0);
    const i = find();
    return this.answer(q, i >= 0 ? i : this.last(q), known);
  }

  private async act(qid: string, known: number, fn: (e: Execution) => Promise<unknown>) {
    const q = this.get(qid);
    try {
      await fn(q.execution);
    } catch (e) {
      q.error = `${(e as Error).name}: ${(e as Error).message}`;
    }
    return this.answer(q, this.last(q), known);
  }

  wave(qid: string, known = 0) {
    return this.act(qid, known, (e) => e.wave());
  }

  finish(qid: string, known = 0, progress?: Progress) {
    return this.act(qid, known, (e) => e.runConcurrent(this.ticker(qid, progress)));
  }

  run(qid: string, known = 0, progress?: Progress) {
    return this.act(qid, known, (e) => e.run(this.ticker(qid, progress)));
  }

  /** A failure: worker `w` stops now (only at the latest step, while the query runs).
   * The coordinator handles WorkerGone at once, so its step comes back. */
  stopWorker(qid: string, w: number, known = 0) {
    const q = this.get(qid);
    q.execution.stopWorker(w);
    return this.answer(q, this.last(q), known);
  }

  /** Progress while a long run goes on: at most a few times a second. */
  private ticker(qid: string, progress?: Progress): () => void {
    let last = performance.now();
    return () => {
      const now = performance.now();
      if (!progress || now - last < 200) return;
      last = now;
      const e = this.get(qid).execution;
      progress({ phase: `step ${e.events.length}`, ...e.progress() });
    };
  }

  state(qid: string, at?: number) {
    const q = this.get(qid);
    return this.snapshot(q, at ?? this.last(q));
  }

  close(qid: string) {
    this.queries.delete(qid);
  }

  /** Everything the query views show, right after step `at`. */
  private snapshot(q: OpenQuery, at: number) {
    const st = q.execution.state(at) as Record<string, unknown>;
    const opNode = (op: PhysicalOperator) => (op.planNode ? (q.nodeIds.get(op.planNode) ?? null) : null);
    const pipelines = (st.pipelines as Record<string, unknown>[]).map((p, i) => ({
      ...p,
      ops: (p.ops as Record<string, unknown>[]).map((o, j) => {
        const op = q.physical.pipelines[i]!.chain[j]!;
        return {
          ...o, node: opNode(op), state: toJson(o.state),
          reads: op.reads().map((x) => x.index), writes: "writes" in op ? ((op as Sink).writes()?.index ?? null) : null,
          role: j === 0 ? "source" : j === q.physical.pipelines[i]!.chain.length - 1 ? "sink" : "operator",
        };
      }),
    }));
    const total = st.result_rows as number;
    const failed = st.failed as { error: string } | null;
    const latest = at >= this.last(q);
    return {
      ...st, pipelines, error: failed?.error ?? (latest ? q.error : null),
      result: { rows: q.execution.rows().slice(0, Math.min(total, MAX_RESULT_ROWS)) as Value[][], total },
    };
  }
}
