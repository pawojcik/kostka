// The physical planner: turns an optimized logical plan into pipelines
// (`source -> operator* -> sink`) joined by output buffers. It reads no data.
//
//   Scan               a ScanSource starts a pipeline
//   Filter, Project    appended to the current pipeline
//   Limit              appended where the pipeline has one split; else two-phase: a
//                      local Limit per split, a gather that needs only n rows, and a
//                      final Limit in a pipeline reading the gather as one split
//   Join               the build (right) side ends in an output buffer:
//                      - broadcast (one worker, no keys, or (workers - 1) x build
//                        <= probe): the probe side continues the current pipeline
//                      - shuffle: both sides are shuffled by key into N partitions;
//                        a new pipeline reads the probe side's partitions
//                      HashProbeOp joins; an inner join can add runtime filters
//   Aggregate          PartialAggregateSink shuffles partial groups by key (gathers
//                      them for a global aggregate); MergeAggregateSource starts the
//                      next pipeline
//   Sort / TopN        SortSink / TopNSink write sorted runs to a gather;
//                      MergeSortSource merges them
//   root               ResultSink
//
// For a new logical node, register a function that compiles it: `registerPlanner`.

import { internal } from "../core/errors.ts";
import { Alias, ColumnRef, Field, conjuncts, isCall, refIds, subsetOf, type AggCall, type Expr } from "../core/expr.ts";
import { andAll } from "../core/functions/index.ts";
import { Estimator } from "../sql/optimizer/cardinality.ts";
import { Aggregate, Filter, Join, Limit, Project, Scan, Sort, TopN, idsOf, type PlanNode } from "../sql/plan.ts";
import { OutputBuffer, type BufferMode } from "./buffer.ts";
import type { Operator, PhysicalOperator, Sink, Source } from "./operator.ts";
import { AggLayout, MergeAggregateSource, PartialAggregateSink } from "./operators/aggregate.ts";
import { BufferSink, BufferSource, ResultSink } from "./operators/buffer-ops.ts";
import { HashProbeOp } from "./operators/join.ts";
import { RuntimeFilter } from "./operators/runtime-filter.ts";
import { ScanSource, type TableReader } from "./operators/scan.ts";
import { MergeSortSource, SortSink, TopNSink } from "./operators/sort.ts";
import { FilterOp, LimitOp, ProjectOp, RuntimeFilterOp } from "./operators/streaming.ts";
import { PhysicalPlan, Pipeline, type LargestNode, type PlannerSettings } from "./pipeline.ts";

export const DEFAULT_SETTINGS: PlannerSettings = { workers: 1, shufflePartitions: "auto", joinStrategy: "auto", runtimeFilters: true };

/** The shuffle partitions settings give: "auto" is 2 per worker. */
export const shufflePartitions = (s: Pick<PlannerSettings, "workers" | "shufflePartitions">): number =>
  s.shufflePartitions === "auto" ? 2 * s.workers : s.shufflePartitions;

/** A pipeline under construction. */
export interface Open {
  source: Source;
  operators: Operator[];
  dependsOn: number[];
}

export type NodePlanner = (node: PlanNode, b: PlanBuilder) => Open;
const planners = new Map<string, NodePlanner>();

/** Teaches the planner a new logical node kind (by class name). */
export function registerPlanner(kind: string, fn: NodePlanner): void {
  planners.set(kind, fn);
}

export function plan(root: PlanNode, reader: TableReader, settings: Partial<PlannerSettings> = {}): PhysicalPlan {
  const merged = { ...DEFAULT_SETTINGS, ...settings };
  const s = { ...merged, shufflePartitions: shufflePartitions(merged) };
  const b = new PlanBuilder(reader, s);
  const open = b.build(root);
  const result = new ResultSink(root.schema);
  b.tag(result, root);
  b.close(open, result);
  return new PhysicalPlan(b.pipelines, b.buffers, b.filters, root.schema, s, largest(root));
}

/** The node with the largest row estimate, for the coordinator's size check. */
function largest(root: PlanNode): LargestNode | null {
  const est = new Estimator();
  let best: LargestNode | null = null;
  const walk = (n: PlanNode): void => {
    let rows = -1;
    try {
      rows = est.rows(n);
    } catch {
      // no estimate for this node
    }
    if (rows > (best?.rows ?? -1)) {
      const detail = n.detail();
      best = { rows, node: `${n.kind}${detail ? ` ${detail.length > 60 ? `${detail.slice(0, 57)}…` : detail}` : ""}`, cross: n instanceof Join && n.condition === null };
    }
    n.children().forEach(walk);
  };
  walk(root);
  return best;
}

export class PlanBuilder {
  readonly reader: TableReader;
  readonly settings: PlannerSettings & { readonly shufflePartitions: number };
  readonly pipelines: Pipeline[] = [];
  readonly buffers: OutputBuffer[] = [];
  readonly filters: RuntimeFilter[] = [];
  private readonly estimator = new Estimator();

  constructor(reader: TableReader, settings: PlannerSettings & { readonly shufflePartitions: number }) {
    this.reader = reader;
    this.settings = settings;
  }

  buffer(mode: BufferMode, n: number, columns: OutputBuffer["columns"], label: string): OutputBuffer {
    const b = new OutputBuffer(this.buffers.length, mode, n, columns, label);
    this.buffers.push(b);
    return b;
  }

  tag<T extends PhysicalOperator>(op: T, node: PlanNode): T {
    op.planNode = node;
    return op;
  }

  /** Ends `p` in `sink`; returns the new pipeline's index. */
  close(p: Open, sink: Sink): number {
    const index = this.pipelines.length;
    const pipeline = new Pipeline(index, p.source, p.operators, sink, [...new Set(p.dependsOn)].sort((a, b) => a - b));
    this.pipelines.push(pipeline);
    const written = sink.writes();
    if (written) written.producer = index;
    for (const op of pipeline.chain) {
      for (const b of op.reads()) if (!b.consumers.includes(index)) b.consumers.push(index);
    }
    if (p.source instanceof ScanSource) for (const rf of p.source.runtimeFilters) rf.consumer = index;
    return index;
  }

  build(node: PlanNode): Open {
    const custom = planners.get(node.kind);
    if (custom) return custom(node, this);
    if (node instanceof Scan) return { source: this.tag(new ScanSource(node, this.reader), node), operators: [], dependsOn: [] };
    if (node instanceof Filter) return this.append(node.child, new FilterOp(node.condition, node.child.schema), node);
    if (node instanceof Project) return this.append(node.child, new ProjectOp(node.exprs, node.child.schema, node.schema), node);
    if (node instanceof Limit) return this.limit(node);
    if (node instanceof Aggregate) return this.aggregate(node);
    if (node instanceof Sort || node instanceof TopN) return this.sort(node);
    if (node instanceof Join) return this.join(node);
    throw internal(`no physical plan for ${node.kind}`);
  }

  private append(child: PlanNode, op: Operator, node: PlanNode): Open {
    const p = this.build(child);
    p.operators.push(this.tag(op, node));
    return p;
  }

  /** A Limit counts within a split: exact where the pipeline has one split, else the
   * local half of a two-phase limit. */
  private limit(node: Limit): Open {
    const p = this.build(node.child);
    p.operators.push(this.tag(new LimitOp(node.n, node.schema), node));
    if (p.source.oneSplit()) return p;
    const ex = this.buffer("gather", 1, node.schema, "limited rows");
    ex.enough = node.n;
    const done = this.close(p, this.tag(new BufferSink(ex, [], node.schema), node));
    return { source: this.tag(new BufferSource(ex, true), node), operators: [this.tag(new LimitOp(node.n, node.schema), node)], dependsOn: [done] };
  }

  private aggregate(node: Aggregate): Open {
    const calls = node.aggs.map((a) => (a instanceof Alias ? a.child : a) as AggCall);
    const p = this.build(node.child);
    const [mode, n] = node.keys.length ? (["shuffle", this.settings.shufflePartitions] as const) : (["gather", 1] as const);
    const layout = new AggLayout(calls);
    // The buffer carries each aggregate's state, not its result: e.g. avg's (count, sum).
    const stateFields = node.schema.slice(node.keys.length).flatMap((f, i) =>
      layout.impls[i]!.state.map((t, j) => new Field(null, `${f.name}[${j}]`, -(f.id * 16 + j), t)));
    const ex = this.buffer(mode, n, [...node.schema.slice(0, node.keys.length), ...stateFields], "partial aggregates");
    const sink = this.tag(new PartialAggregateSink(ex, node.keys, calls, node.child.schema), node);
    const done = this.close(p, sink);
    return { source: this.tag(new MergeAggregateSource(ex, node.keys.length, sink.layout, node.schema), node), operators: [], dependsOn: [done] };
  }

  private sort(node: Sort | TopN): Open {
    const p = this.build(node.child);
    const ex = this.buffer("gather", 1, node.schema, "sorted runs");
    const topn = node instanceof TopN;
    const sink = topn ? new TopNSink(ex, node.keys, node.n, node.child.schema) : new SortSink(ex, node.keys, node.child.schema);
    const done = this.close(p, this.tag(sink, node));
    return { source: this.tag(new MergeSortSource(ex, node.keys, topn ? node.n : null), node), operators: [], dependsOn: [done] };
  }

  private strategy(node: Join, hasKeys: boolean): "broadcast" | "shuffle" {
    if (!hasKeys) return "broadcast";
    if (this.settings.joinStrategy !== "auto") return this.settings.joinStrategy;
    if (this.settings.workers === 1) return "broadcast";
    const build = this.estimator.rows(node.right), probe = this.estimator.rows(node.left);
    return (this.settings.workers - 1) * build <= probe ? "broadcast" : "shuffle";
  }

  private join(node: Join): Open {
    const { leftKeys, rightKeys, residual } = splitJoinCondition(node.condition, idsOf(node.left), idsOf(node.right));
    const right = this.build(node.right);
    const mode = this.strategy(node, leftKeys.length > 0);
    const n = mode === "broadcast" ? 1 : this.settings.shufflePartitions;
    const bx = this.buffer(mode, n, node.right.schema, "build side");
    const buildSink = this.tag(new BufferSink(bx, rightKeys, node.right.schema), node);
    const built = this.close(right, buildSink);
    let p = this.build(node.left);
    const filtered = this.settings.runtimeFilters && node.how === "inner" && this.runtimeFilters(node, p, leftKeys, buildSink, built);
    if (mode === "broadcast" || filtered) p.dependsOn.push(built);
    if (mode === "shuffle") {
      const px = this.buffer("shuffle", n, node.left.schema, "probe side");
      px.partitioning = bx.partitioning; // co-partitioned: partition p of both meets on one worker
      const sent = this.close(p, this.tag(new BufferSink(px, leftKeys, node.left.schema), node));
      p = { source: this.tag(new BufferSource(px), node), operators: [], dependsOn: [sent, built] };
    }
    p.operators.push(this.tag(new HashProbeOp(bx, rightKeys, leftKeys, residual, node.how, node.left.schema, node.right.schema), node));
    return p;
  }

  /** Lets the probe pipeline's scan skip files and rows that cannot match this inner
   * join: for each probe key that is a plain column of that scan. The row filter
   * only when another probe runs before this one in the pipeline. */
  private runtimeFilters(join: Join, p: Open, leftKeys: readonly Expr[], sink: BufferSink, built: number): boolean {
    if (!(p.source instanceof ScanSource)) return false;
    const scan = p.source.planNode as Scan;
    const rowFilter = p.operators.some((op) => op instanceof HashProbeOp);
    let added = false;
    leftKeys.forEach((key, pos) => {
      if (!(key instanceof ColumnRef)) return;
      const found = scanColumn(join.left, key.id);
      if (!found || found.scan !== scan) return;
      const rf = new RuntimeFilter(this.filters.length, pos, `${found.column} ∈ keys of P${built}`, built, found.column);
      this.filters.push(rf);
      sink.runtimeFilters.push(rf);
      (p.source as ScanSource).runtimeFilters.push(rf);
      added = true;
      if (rowFilter) {
        let first = p.operators[0];
        if (!(first instanceof RuntimeFilterOp)) {
          first = this.tag(new RuntimeFilterOp(scan.schema), join);
          p.operators.unshift(first);
        }
        (first as RuntimeFilterOp).filters.push([found.slot, rf]);
      }
    });
    return added;
  }
}

/** (scan, column, slot) if column `id` of `node`'s output comes unchanged from a Scan
 * in the same pipeline: through Filters, pass-through Project columns and the probe
 * side of joins. Not through Limit, TopN, Aggregate or Sort. */
function scanColumn(node: PlanNode, id: number): { scan: Scan; column: string; slot: number } | null {
  for (;;) {
    if (node instanceof Scan) {
      const slot = node.schema.findIndex((f) => f.id === id);
      return slot < 0 ? null : { scan: node, column: node.info.columns[slot]!, slot };
    }
    if (node instanceof Filter) node = node.child;
    else if (node instanceof Project) {
      const i = node.schema.findIndex((f) => f.id === id);
      if (i < 0) return null;
      let e = node.exprs[i]!;
      if (e instanceof Alias) e = e.child;
      if (!(e instanceof ColumnRef)) return null;
      id = e.id;
      node = node.child;
    } else if (node instanceof Join) {
      if (!idsOf(node.left).has(id)) return null;
      node = node.left;
    } else return null;
  }
}

/** Splits a join condition into hash keys and a residual: each top-level `l = r`
 * conjunct with one side on each input becomes a key pair. */
export function splitJoinCondition(cond: Expr | null, leftIds: Set<number>, rightIds: Set<number>) {
  const leftKeys: Expr[] = [], rightKeys: Expr[] = [], rest: Expr[] = [];
  if (!cond) return { leftKeys, rightKeys, residual: null };
  const side = (e: Expr) => {
    const ids = refIds(e);
    if (ids.size && subsetOf(ids, leftIds)) return "left";
    if (ids.size && subsetOf(ids, rightIds)) return "right";
    return null;
  };
  for (const c of conjuncts(cond)) {
    if (isCall(c, "=")) {
      const [a, b] = c.args as [Expr, Expr];
      const sa = side(a), sb = side(b);
      if (sa && sb && sa !== sb) {
        leftKeys.push(sa === "left" ? a : b);
        rightKeys.push(sa === "left" ? b : a);
        continue;
      }
    }
    rest.push(c);
  }
  return { leftKeys, rightKeys, residual: andAll(rest) };
}

