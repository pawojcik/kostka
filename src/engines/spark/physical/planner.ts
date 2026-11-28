// Spark's physical planner: turns the optimized plan into a Spark plan (plan.ts). It
// reads no data. Each logical node becomes Spark's operators:
//
//   Scan                 FileScan (one partition per data file)
//   Filter, Project      Filter, Project
//   Aggregate            HashAggregate(partial) -> Exchange -> HashAggregate(final)
//                        (hash on the keys; SinglePartition without keys)
//   Join, equi-keys      BroadcastHashJoin when the build side's estimated bytes are at
//                        most broadcastThreshold; else SortMergeJoin, with a hash
//                        Exchange and a Sort on each side
//   Join, no keys        BroadcastNestedLoopJoin
//   Sort                 Exchange SinglePartition -> Sort
//   TopN                 TakeOrderedAndProject(partial) -> Exchange SinglePartition
//                        -> TakeOrderedAndProject(final)
//   Limit                LocalLimit -> Exchange SinglePartition -> GlobalLimit
//
// As Spark's EnsureRequirements does, an Exchange goes in only where the child's
// partitioning is not what its parent needs: an operator that needs one partition gets
// none if its child already is one (a Limit above a Sort, say).
//
// The broadcast choice. The build side is the right input (the optimizer put the
// smaller side there). A left join keeps the left side's unmatched rows, so the right
// side can always be broadcast (Spark's BuildRight). Its estimated bytes are the
// optimizer's row estimate times the row width of its columns (core/rows.ts), compared
// with a fixed threshold, like Spark's autoBroadcastJoinThreshold. -1 turns broadcasts
// off (a join without keys still broadcasts: it has no other way to run).

import { AggLayout } from "../../../core/accumulators.ts";
import { AggCall, Alias, Field, type Expr } from "../../../core/expr.ts";
import { internal } from "../../../core/errors.ts";
import { fieldsBytes } from "../../../core/rows.ts";
import { Estimator } from "../../../sql/optimizer/cardinality.ts";
import { Aggregate, Filter, Join, Limit, Project, Scan, Sort, TopN, idsOf, splitJoinCondition, type PlanNode } from "../../../sql/plan.ts";
import type { TableReader } from "../../../storage/table-reader.ts";
import { HashAggregateExec } from "./operators/aggregate.ts";
import { FilterExec, GlobalLimitExec, LocalLimitExec, ProjectExec } from "./operators/basic.ts";
import { BroadcastExchange, ShuffleExchange } from "./operators/exchange.ts";
import { BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec, joinSortKeys } from "./operators/join.ts";
import { FileScan } from "./operators/scan.ts";
import { SortExec, TakeOrderedAndProjectExec } from "./operators/sort.ts";
import { SparkOp, explainTree, hashedBy, walk } from "./plan.ts";

export interface PlanSettings {
  /** Bytes; a build side estimated at most this big is broadcast. -1: never. */
  readonly broadcastThreshold: number;
  /** Partitions of every hash Exchange. */
  readonly shufflePartitions: number;
}

/** How a join was planned, and why: for the plan view. */
export interface JoinChoice {
  readonly op: number;
  readonly strategy: "BroadcastHashJoin" | "SortMergeJoin" | "BroadcastNestedLoopJoin";
  /** The build side's estimated bytes (null: a join without keys). */
  readonly buildBytes: number | null;
  readonly threshold: number;
}

export class SparkPlan {
  readonly root: SparkOp;
  /** Every operator, by id (preorder). */
  readonly ops: readonly SparkOp[];
  readonly shuffles: readonly ShuffleExchange[];
  readonly broadcasts: readonly BroadcastExchange[];
  readonly joins: readonly JoinChoice[];
  readonly settings: PlanSettings;
  constructor(root: SparkOp, shuffles: readonly ShuffleExchange[], broadcasts: readonly BroadcastExchange[], joins: readonly JoinChoice[], settings: PlanSettings) {
    this.root = root;
    this.ops = [...walk(root)];
    this.shuffles = shuffles;
    this.broadcasts = broadcasts;
    this.joins = joins;
    this.settings = settings;
  }
  explain(): string {
    return explainTree(this.root);
  }
}

export function planSpark(root: PlanNode, reader: TableReader, settings: PlanSettings): SparkPlan {
  const p = new Planner(reader, settings);
  const top = p.plan(root);
  [...walk(top)].forEach((op, i) => (op.id = i));
  const joins = p.joins.map(([op, c]) => ({ ...c, op: op.id }));
  return new SparkPlan(top, p.shuffles, p.broadcasts, joins, settings);
}

class Planner {
  private readonly reader: TableReader;
  private readonly settings: PlanSettings;
  private readonly estimator = new Estimator();
  readonly shuffles: ShuffleExchange[] = [];
  readonly broadcasts: BroadcastExchange[] = [];
  readonly joins: [SparkOp, Omit<JoinChoice, "op">][] = [];

  constructor(reader: TableReader, settings: PlanSettings) {
    this.reader = reader;
    this.settings = settings;
  }

  private tag<T extends SparkOp>(op: T, node: PlanNode): T {
    op.planNode = node;
    return op;
  }

  private shuffle(child: SparkOp, keys: readonly Expr[] | null, node: PlanNode): ShuffleExchange {
    const ex = this.tag(new ShuffleExchange(child, keys ? { kind: "hash", keys, n: this.settings.shufflePartitions } : { kind: "single" }), node);
    ex.shuffle = this.shuffles.length;
    this.shuffles.push(ex);
    return ex;
  }

  private broadcast(child: SparkOp, node: PlanNode): BroadcastExchange {
    const ex = this.tag(new BroadcastExchange(child), node);
    ex.broadcast = this.broadcasts.length;
    this.broadcasts.push(ex);
    return ex;
  }

  /** `op`, or an Exchange to one partition above it if it has several. */
  private single(op: SparkOp, node: PlanNode): SparkOp {
    return op.partitioning().kind === "single" ? op : this.shuffle(op, null, node);
  }

  /** `op`, or a hash Exchange on `keys` above it if it is not partitioned so. */
  private hashed(op: SparkOp, keys: readonly Expr[], node: PlanNode): SparkOp {
    return hashedBy(op.partitioning(), keys, this.settings.shufflePartitions) ? op : this.shuffle(op, keys, node);
  }

  plan(node: PlanNode): SparkOp {
    if (node instanceof Scan) return this.tag(new FileScan(node, this.reader), node);
    if (node instanceof Filter) return this.tag(new FilterExec(this.plan(node.child), node.condition), node);
    if (node instanceof Project) return this.tag(new ProjectExec(this.plan(node.child), node.exprs, node.schema), node);
    if (node instanceof Aggregate) return this.aggregate(node);
    if (node instanceof Join) return this.join(node);
    if (node instanceof Sort) return this.tag(new SortExec(this.single(this.plan(node.child), node), node.keys, true), node);
    if (node instanceof TopN) {
      const child = this.plan(node.child);
      if (child.partitioning().kind === "single") return this.tag(new TakeOrderedAndProjectExec(child, node.keys, node.n, "final"), node);
      const partial = this.tag(new TakeOrderedAndProjectExec(child, node.keys, node.n, "partial"), node);
      return this.tag(new TakeOrderedAndProjectExec(this.shuffle(partial, null, node), node.keys, node.n, "final"), node);
    }
    if (node instanceof Limit) {
      const child = this.plan(node.child);
      if (child.partitioning().kind === "single") return this.tag(new GlobalLimitExec(child, node.n), node);
      return this.tag(new GlobalLimitExec(this.shuffle(this.tag(new LocalLimitExec(child, node.n), node), null, node), node.n), node);
    }
    throw internal(`the Spark-style engine has no plan for ${node.kind}`);
  }

  private aggregate(node: Aggregate): SparkOp {
    const calls = node.aggs.map((a) => (a instanceof Alias ? a.child : a)).filter((a): a is AggCall => a instanceof AggCall);
    if (calls.length !== node.aggs.length) throw internal("an aggregate that is not an AggCall");
    const nKeys = node.keys.length;
    const layout = new AggLayout(calls);
    // The partial rows carry each aggregate's state, not its result: e.g. avg's (count, sum).
    const stateFields = node.schema.slice(nKeys).flatMap((f, i) =>
      layout.impls[i]!.state.map((t, j) => new Field(null, `${f.name}[${j}]`, -(f.id * 16 + j), t)));
    const partial = this.tag(new HashAggregateExec(this.plan(node.child), "partial", node.keys, calls, [...node.schema.slice(0, nKeys), ...stateFields]), node);
    const keys = partial.output.slice(0, nKeys).map((f) => f.ref());
    // One partition holds every group whole already: a final aggregate needs no Exchange then.
    const input = nKeys && partial.partitioning().kind !== "single" ? this.hashed(partial, keys, node) : this.single(partial, node);
    return this.tag(new HashAggregateExec(input, "final", keys, calls, node.schema), node);
  }

  private join(node: Join): SparkOp {
    const { leftKeys, rightKeys, residual } = splitJoinCondition(node.condition, idsOf(node.left), idsOf(node.right));
    const left = this.plan(node.left), right = this.plan(node.right);
    const threshold = this.settings.broadcastThreshold;
    if (!leftKeys.length) {
      const op = this.tag(new BroadcastNestedLoopJoinExec(left, this.broadcast(right, node), node.how, residual), node);
      this.joins.push([op, { strategy: "BroadcastNestedLoopJoin", buildBytes: null, threshold }]);
      return op;
    }
    const buildBytes = Math.round(this.estimator.rows(node.right) * fieldsBytes(node.right.schema));
    if (threshold >= 0 && buildBytes <= threshold) {
      const op = this.tag(new BroadcastHashJoinExec(left, this.broadcast(right, node), leftKeys, rightKeys, node.how, residual), node);
      this.joins.push([op, { strategy: "BroadcastHashJoin", buildBytes, threshold }]);
      return op;
    }
    const sortedBy = (op: SparkOp, keys: readonly Expr[]) => this.tag(new SortExec(this.hashed(op, keys, node), joinSortKeys(keys), false), node);
    const op = this.tag(new SortMergeJoinExec(sortedBy(left, leftKeys), sortedBy(right, rightKeys), leftKeys, rightKeys, node.how, residual), node);
    this.joins.push([op, { strategy: "SortMergeJoin", buildBytes, threshold }]);
    return op;
  }
}
