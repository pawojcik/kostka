// Logical plan nodes. The analyzer builds them from a query, fully resolved: every
// node has its output `schema` (Fields with expression ids and types), and every
// expression is bound. Nodes are immutable; rewrites build new ones.
//
//   Scan(table, snapshot)        reads one pinned snapshot of a table
//   Filter(child, condition)
//   Project(child, exprs)        computes its output columns
//   Aggregate(child, keys, aggs) GROUP BY keys; output = keys, then aggregates
//   Join(left, right, how, on)   inner, left or cross; `on` is null for cross
//   Sort(child, keys)
//   Limit(child, n)
//   TopN(child, keys, n)         Sort + Limit, fused by the optimizer
//
// To add a node: subclass PlanNode, implement `children`, `withChildren` (which
// recomputes a pass-through schema) and `detail`, and give the physical planner a
// way to compile it (physical/planner.ts).

import { AggCall, Alias, type Expr, Field, type SortKey } from "../core/expr.ts";
import type { ColumnType } from "../core/types.ts";
import type { DataFile, TableMeta } from "../catalog/meta.ts";

export abstract class PlanNode {
  abstract readonly schema: readonly Field[];
  abstract children(): readonly PlanNode[];
  abstract withChildren(children: readonly PlanNode[]): PlanNode;
  /** One-line description for EXPLAIN (without the node name and columns). */
  abstract detail(): string;
  get kind(): string {
    return this.constructor.name;
  }
}

export interface ScanInfo {
  readonly table: string;
  readonly meta: TableMeta;
  readonly version: number;
  /** Column names and types in table order (narrowed by projection pushdown). */
  readonly columns: readonly string[];
  readonly types: readonly ColumnType[];
  /** The files it reads (narrowed by file pruning), and all of the snapshot's. */
  readonly files: readonly DataFile[];
  readonly snapshotFiles: readonly DataFile[];
  readonly alias: string | null;
}

export class Scan extends PlanNode {
  readonly info: ScanInfo;
  readonly schema: readonly Field[];
  constructor(info: ScanInfo, schema: readonly Field[]) {
    super();
    this.info = info;
    this.schema = schema;
  }
  get table(): string {
    return this.info.table;
  }
  children(): readonly PlanNode[] {
    return [];
  }
  withChildren(): PlanNode {
    return this;
  }
  /** A copy reading only the columns at `keep` and/or only `files`. */
  derive({ keep, files }: { keep?: readonly number[]; files?: readonly DataFile[] }): Scan {
    const idx = keep ?? this.schema.map((_, i) => i);
    const info: ScanInfo = {
      ...this.info,
      columns: idx.map((i) => this.info.columns[i]!),
      types: idx.map((i) => this.info.types[i]!),
      files: files ?? this.info.files,
    };
    return new Scan(info, idx.map((i) => this.schema[i]!));
  }
  detail(): string {
    const { table, version, alias, files, snapshotFiles } = this.info;
    const pruned = files.length === snapshotFiles.length ? "" : ` files ${files.length}/${snapshotFiles.length}`;
    return `${table}@v${version}${alias && alias !== table ? ` AS ${alias}` : ""}${pruned}`;
  }
}

export class Filter extends PlanNode {
  readonly child: PlanNode;
  readonly condition: Expr;
  constructor(child: PlanNode, condition: Expr) {
    super();
    this.child = child;
    this.condition = condition;
  }
  get schema(): readonly Field[] {
    return this.child.schema;
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Filter(c[0]!, this.condition);
  }
  detail(): string {
    return String(this.condition);
  }
}

export class Project extends PlanNode {
  readonly child: PlanNode;
  readonly exprs: readonly Expr[];
  readonly schema: readonly Field[];
  constructor(child: PlanNode, exprs: readonly Expr[], schema: readonly Field[]) {
    super();
    this.child = child;
    this.exprs = exprs;
    this.schema = schema;
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Project(c[0]!, this.exprs, this.schema);
  }
  detail(): string {
    return this.exprs.map(String).join(", ");
  }
}

export class Aggregate extends PlanNode {
  readonly child: PlanNode;
  readonly keys: readonly Expr[];
  /** AggCalls, each possibly under an Alias. */
  readonly aggs: readonly Expr[];
  readonly schema: readonly Field[];
  constructor(child: PlanNode, keys: readonly Expr[], aggs: readonly Expr[], schema: readonly Field[]) {
    super();
    this.child = child;
    this.keys = keys;
    this.aggs = aggs;
    this.schema = schema;
  }
  aggCalls(): AggCall[] {
    return this.aggs.map((a) => (a instanceof Alias ? a.child : a) as AggCall);
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Aggregate(c[0]!, this.keys, this.aggs, this.schema);
  }
  detail(): string {
    return `keys [${this.keys.map(String).join(", ")}] aggs [${this.aggs.map(String).join(", ")}]`;
  }
}

export type JoinType = "inner" | "left" | "cross";

export class Join extends PlanNode {
  readonly left: PlanNode;
  readonly right: PlanNode;
  readonly how: JoinType;
  readonly condition: Expr | null;
  constructor(left: PlanNode, right: PlanNode, how: JoinType, condition: Expr | null) {
    super();
    this.left = left;
    this.right = right;
    this.how = how;
    this.condition = condition;
  }
  get schema(): readonly Field[] {
    return [...this.left.schema, ...this.right.schema];
  }
  children(): readonly PlanNode[] {
    return [this.left, this.right];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Join(c[0]!, c[1]!, this.how, this.condition);
  }
  detail(): string {
    return this.how + (this.condition ? ` on ${this.condition}` : "");
  }
}

export class Sort extends PlanNode {
  readonly child: PlanNode;
  readonly keys: readonly SortKey[];
  constructor(child: PlanNode, keys: readonly SortKey[]) {
    super();
    this.child = child;
    this.keys = keys;
  }
  get schema(): readonly Field[] {
    return this.child.schema;
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Sort(c[0]!, this.keys);
  }
  detail(): string {
    return this.keys.map(String).join(", ");
  }
}

export class Limit extends PlanNode {
  readonly child: PlanNode;
  readonly n: number;
  constructor(child: PlanNode, n: number) {
    super();
    this.child = child;
    this.n = n;
  }
  get schema(): readonly Field[] {
    return this.child.schema;
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new Limit(c[0]!, this.n);
  }
  detail(): string {
    return String(this.n);
  }
}

export class TopN extends PlanNode {
  readonly child: PlanNode;
  readonly keys: readonly SortKey[];
  readonly n: number;
  constructor(child: PlanNode, keys: readonly SortKey[], n: number) {
    super();
    this.child = child;
    this.keys = keys;
    this.n = n;
  }
  get schema(): readonly Field[] {
    return this.child.schema;
  }
  children(): readonly PlanNode[] {
    return [this.child];
  }
  withChildren(c: readonly PlanNode[]): PlanNode {
    return new TopN(c[0]!, this.keys, this.n);
  }
  detail(): string {
    return `${this.n} by ${this.keys.map(String).join(", ")}`;
  }
}

/** An indented tree of the plan with every node's columns. */
export function explain(plan: PlanNode): string {
  const lines: string[] = [];
  const visit = (n: PlanNode, depth: number) => {
    const cols = n.schema.map((f) => f.label()).join(", ");
    lines.push(`${"  ".repeat(depth)}${n.kind} ${n.detail()} -> [${cols}]`);
    for (const c of n.children()) visit(c, depth + 1);
  };
  visit(plan, 0);
  return lines.join("\n");
}

/** Every node of a plan, parents before children. */
export function* nodes(plan: PlanNode): Generator<PlanNode> {
  yield plan;
  for (const c of plan.children()) yield* nodes(c);
}

export const idsOf = (n: PlanNode): Set<number> => new Set(n.schema.map((f) => f.id));
