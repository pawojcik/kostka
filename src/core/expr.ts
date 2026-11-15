// Bound, typed expressions: what the analyzer produces from SQL and the optimizer and
// the physical planner work with. Every node knows its result type.
//
// Columns are referenced by expression id, never by position (see `Field`): a
// `ColumnRef` names the id of the column it reads, so plan rewrites can move
// expressions between nodes without renumbering. Operators resolve ids to row slots
// once, when an expression is compiled into an evaluator (compile.ts).
//
// Every node implements `Expression`: its type, its children, how it compiles into an
// evaluator and how it prints. Nothing outside the node switches on its class to do
// those, so a new kind of expression is one new class.
//
// Node kinds:
//   ColumnRef(id, name, type)    a column of the input
//   Literal(value, type)
//   Call(fn, args, bound)        a scalar function or operator from the registry
//                                (`+`, `=`, `and`, `upper`, ...), bound to its
//                                argument types
//   Cast(child, type, implicit)  implicit casts are the widenings the analyzer adds
//                                (int -> float, date -> timestamp)
//   Case(whens, otherwise)       CASE WHEN c THEN x ... ELSE y END
//   AggCall(fn, arg, bound)      an aggregate; only in an Aggregate node's list
//   Alias(child, name)           names a projected or aggregated expression

import { internal } from "./errors.ts";
import type { AggregateFunction, BoundAggregate } from "./functions/aggregates.ts";
import { castImpl } from "./functions/cast.ts";
import type { BoundFunction, Evaluator, ScalarFunction } from "./functions/registry.ts";
import { sqlLiteral, type DataType, type Value } from "./types.ts";

/** One column of a plan node's output. `id` is its expression id: unique within a
 * plan, the column's identity. `qualifier` is the table or alias it can be named by. */
export class Field {
  readonly qualifier: string | null;
  readonly name: string;
  readonly id: number;
  readonly type: DataType;
  constructor(qualifier: string | null, name: string, id: number, type: DataType) {
    this.qualifier = qualifier;
    this.name = name;
    this.id = id;
    this.type = type;
  }
  ref(): ColumnRef {
    return new ColumnRef(this.id, this.name, this.type);
  }
  /** "t.a#3": how plans and EXPLAIN show it. */
  label(): string {
    return `${this}#${this.id}`;
  }
  withQualifier(q: string | null): Field {
    return new Field(q, this.name, this.id, this.type);
  }
  toString(): string {
    return this.qualifier === null ? this.name : `${this.qualifier}.${this.name}`;
  }
}

/** What compiling an expression needs from its surroundings (see compile.ts). */
export interface CompileContext {
  /** The row slot holding the column with this id; an internal error if none does. */
  slot(ref: ColumnRef): number;
  /** Compiles a child expression in the same context. */
  compile(e: Expression): Evaluator;
}

/** A bound, typed expression. */
export interface Expression {
  readonly type: DataType;
  children(): readonly Expression[];
  /** A copy with new children (same arity), or this node if it has none. */
  withChildren(children: readonly Expression[]): Expression;
  /** An evaluator computing the value from a row. */
  compile(ctx: CompileContext): Evaluator;
  /** The text of this node, given its children's text (`child`). `ids` says whether
   * column references show their ids. */
  format(child: (e: Expression) => string, ids: boolean): string;
  /** Text with ids (`a#3 + 1`): distinct for distinct expressions, so it also works
   * as a structural key. */
  toString(): string;
  /** Text without ids (`a + 1`): the default name of a projected expression. */
  display(): string;
}

/** The base of the built-in node kinds: leaves by default, and the printing. */
export abstract class Expr implements Expression {
  abstract readonly type: DataType;
  children(): readonly Expr[] {
    return [];
  }
  withChildren(_children: readonly Expr[]): Expr {
    return this;
  }
  abstract compile(ctx: CompileContext): Evaluator;
  abstract format(child: (e: Expr) => string, ids: boolean): string;
  toString(): string {
    return render(this, true);
  }
  display(): string {
    return render(this, false);
  }
}

const render = (e: Expression, ids: boolean): string => e.format((x) => render(x, ids), ids);

export class ColumnRef extends Expr {
  readonly id: number;
  readonly name: string;
  readonly type: DataType;
  constructor(id: number, name: string, type: DataType) {
    super();
    this.id = id;
    this.name = name;
    this.type = type;
  }
  compile(ctx: CompileContext): Evaluator {
    const slot = ctx.slot(this);
    return (row) => row[slot]!;
  }
  format(_: unknown, ids: boolean): string {
    return ids ? `${this.name}#${this.id}` : this.name;
  }
}

export class Literal extends Expr {
  readonly value: Value;
  readonly type: DataType;
  constructor(value: Value, type: DataType) {
    super();
    this.value = value;
    this.type = value === null && type === "null" ? "null" : type;
  }
  compile(): Evaluator {
    const v = this.value;
    return () => v;
  }
  format(): string {
    return sqlLiteral(this.value, this.type);
  }
}

export class Call extends Expr {
  readonly fn: ScalarFunction;
  readonly args: readonly Expr[];
  readonly bound: BoundFunction;
  constructor(fn: ScalarFunction, args: readonly Expr[], bound: BoundFunction) {
    super();
    this.fn = fn;
    this.args = args;
    this.bound = bound;
  }
  get type(): DataType {
    return this.bound.type;
  }
  get name(): string {
    return this.fn.name;
  }
  override children(): readonly Expr[] {
    return this.args;
  }
  override withChildren(children: readonly Expr[]): Expr {
    return new Call(this.fn, children, this.bound);
  }
  compile(ctx: CompileContext): Evaluator {
    return this.bound.compile(this.args.map((a) => ctx.compile(a)));
  }
  format(child: (e: Expr) => string): string {
    const args = this.args.map(child);
    return this.fn.format ? this.fn.format(args) : `${this.fn.name}(${args.join(", ")})`;
  }
}

export class Cast extends Expr {
  readonly child: Expr;
  readonly type: DataType;
  readonly implicit: boolean;
  constructor(child: Expr, type: DataType, implicit = false) {
    super();
    this.child = child;
    this.type = type;
    this.implicit = implicit;
  }
  override children(): readonly Expr[] {
    return [this.child];
  }
  override withChildren(c: readonly Expr[]): Expr {
    return new Cast(c[0]!, this.type, this.implicit);
  }
  compile(ctx: CompileContext): Evaluator {
    const impl = castImpl(this.child.type, this.type);
    if (!impl) throw internal(`no cast from ${this.child.type} to ${this.type}`);
    const x = ctx.compile(this.child);
    return (row) => {
      const v = x(row);
      return v === null ? null : impl(v);
    };
  }
  format(child: (e: Expr) => string): string {
    return this.implicit ? child(this.child) : `CAST(${child(this.child)} AS ${this.type})`;
  }
}

/** CASE WHEN c1 THEN v1 ... ELSE e END; a missing ELSE is NULL. */
export class Case extends Expr {
  readonly whens: readonly (readonly [Expr, Expr])[];
  readonly otherwise: Expr;
  readonly type: DataType;
  constructor(whens: readonly (readonly [Expr, Expr])[], otherwise: Expr, type: DataType) {
    super();
    this.whens = whens;
    this.otherwise = otherwise;
    this.type = type;
  }
  override children(): readonly Expr[] {
    return [...this.whens.flat(), this.otherwise];
  }
  override withChildren(c: readonly Expr[]): Expr {
    const whens = this.whens.map((_, i) => [c[2 * i]!, c[2 * i + 1]!] as const);
    return new Case(whens, c[c.length - 1]!, this.type);
  }
  compile(ctx: CompileContext): Evaluator {
    const whens = this.whens.map(([c, v]) => [ctx.compile(c), ctx.compile(v)] as const);
    const otherwise = ctx.compile(this.otherwise);
    return (row) => {
      for (const [c, v] of whens) if (c(row) === true) return v(row);
      return otherwise(row);
    };
  }
  format(child: (e: Expr) => string): string {
    const whens = this.whens.map(([c, v]) => ` WHEN ${child(c)} THEN ${child(v)}`).join("");
    const o = this.otherwise;
    return `CASE${whens}${o instanceof Literal && o.value === null ? "" : ` ELSE ${child(o)}`} END`;
  }
}

/** An aggregate call: `fn(arg)`, or `count(*)` with arg null. It has no per-row value:
 * the Aggregate operator runs its `bound` state machine instead. */
export class AggCall extends Expr {
  readonly fn: AggregateFunction;
  readonly arg: Expr | null;
  readonly bound: BoundAggregate;
  constructor(fn: AggregateFunction, arg: Expr | null, bound: BoundAggregate) {
    super();
    this.fn = fn;
    this.arg = arg;
    this.bound = bound;
  }
  get type(): DataType {
    return this.bound.type;
  }
  override children(): readonly Expr[] {
    return this.arg ? [this.arg] : [];
  }
  override withChildren(c: readonly Expr[]): Expr {
    return new AggCall(this.fn, c[0] ?? null, this.bound);
  }
  compile(): Evaluator {
    throw internal(`aggregate ${this} cannot be evaluated per row`);
  }
  format(child: (e: Expr) => string): string {
    return `${this.fn.name}(${this.arg ? child(this.arg) : "*"})`;
  }
}

export class Alias extends Expr {
  readonly child: Expr;
  readonly name: string;
  constructor(child: Expr, name: string) {
    super();
    this.child = child;
    this.name = name;
  }
  get type(): DataType {
    return this.child.type;
  }
  override children(): readonly Expr[] {
    return [this.child];
  }
  override withChildren(c: readonly Expr[]): Expr {
    return new Alias(c[0]!, this.name);
  }
  compile(ctx: CompileContext): Evaluator {
    return ctx.compile(this.child);
  }
  format(child: (e: Expr) => string, ids: boolean): string {
    return ids ? `${child(this.child)} AS ${this.name}` : this.name;
  }
}

export class SortKey {
  readonly expr: Expr;
  readonly ascending: boolean;
  readonly nullsFirst: boolean;
  constructor(expr: Expr, ascending = true, nullsFirst = ascending) {
    this.expr = expr;
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
  }
  with(expr: Expr): SortKey {
    return new SortKey(expr, this.ascending, this.nullsFirst);
  }
  toString(): string {
    const nulls = this.nullsFirst === this.ascending ? "" : this.nullsFirst ? " NULLS FIRST" : " NULLS LAST";
    return `${this.expr}${this.ascending ? "" : " DESC"}${nulls}`;
  }
}

// --- helpers ---------------------------------------------------------------------

/** Bottom-up rewrite: `fn` sees each node after its children were rewritten. Returns
 * the same object when nothing changed. */
export function transform(e: Expr, fn: (e: Expr) => Expr): Expr {
  const kids = e.children();
  let node = e;
  if (kids.length) {
    const next = kids.map((k) => transform(k, fn));
    if (next.some((k, i) => k !== kids[i])) node = e.withChildren(next);
  }
  return fn(node);
}

/** Top-down rewrite: `fn` returns a replacement (not descended into) or undefined. */
export function rewriteTopDown(e: Expr, fn: (e: Expr) => Expr | undefined): Expr {
  const hit = fn(e);
  if (hit !== undefined) return hit;
  const kids = e.children();
  if (!kids.length) return e;
  const next = kids.map((k) => rewriteTopDown(k, fn));
  return next.some((k, i) => k !== kids[i]) ? e.withChildren(next) : e;
}

export function* walk(e: Expr): Generator<Expr> {
  yield e;
  for (const k of e.children()) yield* walk(k);
}

export function refIds(e: Expr): Set<number> {
  const out = new Set<number>();
  for (const x of walk(e)) if (x instanceof ColumnRef) out.add(x.id);
  return out;
}

export function refIdsOf(exprs: Iterable<Expr>): Set<number> {
  const out = new Set<number>();
  for (const e of exprs) for (const id of refIds(e)) out.add(id);
  return out;
}

export const containsAgg = (e: Expr): boolean => [...walk(e)].some((x) => x instanceof AggCall);

/** The expression under an Alias. */
export const unalias = (e: Expr): Expr => (e instanceof Alias ? e.child : e);

/** A call of the named built-in (`isCall(e, "and")`). */
export function isCall(e: Expr, name: string): e is Call {
  return e instanceof Call && e.fn.name === name;
}

/** The top-level `and` operands of `e`. */
export function conjuncts(e: Expr): Expr[] {
  return isCall(e, "and") ? [...conjuncts(e.args[0]!), ...conjuncts(e.args[1]!)] : [e];
}

export function subsetOf(a: Set<number>, b: Set<number>): boolean {
  for (const x of a) if (!b.has(x)) return false;
  return true;
}
