// The analyzer: turns a parsed query into a logical plan. It resolves every name
// (tables, CTEs, subqueries, columns), checks types, and builds the plan the SQL
// clauses describe, in their logical order:
//
//   FROM (scans and joins) -> WHERE -> GROUP BY / aggregates -> HAVING
//     -> SELECT (project) -> DISTINCT -> ORDER BY -> LIMIT
//
// Expression ids (see core/expr.ts): every scan column gets a fresh id; a computed
// or aliased output gets a fresh id; a bare column passed through keeps its id. So
// ids are unique within a plan and the optimizer can move expressions freely.
//
// Types: every expression gets one. Functions and operators pick an overload by
// their argument types, with the implicit widenings int -> float and date ->
// timestamp; a string literal compared with a date or timestamp is read as one.
// Anything else that doesn't fit is an AnalysisError pointing at the SQL.
//
// Snapshots: each table is read at one snapshot per query, so every scan of a table
// (a self-join, a CTE used twice) sees the same data.

import { AnalysisError, type Span } from "../core/errors.ts";
import {
  AggCall, Alias, Case, Cast, ColumnRef, Field, Literal, SortKey, containsAgg, rewriteTopDown,
  unalias, walk, type Expr,
} from "../core/expr.ts";
import {
  TypeMismatch, allAggregates, allFunctions, call, castImpl, castValue, coerce, commonType,
  lookupAggregate, lookupFunction,
} from "../core/functions/index.ts";
import { ExecutionError } from "../core/errors.ts";
import {
  isTemporal, parseDate, parseTimestamp, parseType, type ColumnType,
} from "../core/types.ts";
import type { CatalogReader, DataFile, TableMeta } from "../catalog/meta.ts";
import { TableNotFoundError } from "../core/errors.ts";
import type { AstExpr, Cte, FromItem, Query, Select, SelectItem } from "./ast.ts";
import { Aggregate, Filter, Join, Limit, Project, Scan, Sort, type PlanNode } from "./plan.ts";

export function analyze(query: Query, catalog: CatalogReader): PlanNode {
  return new Analyzer(catalog).query(query, new Map()).plan;
}

/** A relation in scope: its plan and the columns it can be referenced by. */
interface Relation {
  plan: PlanNode;
  /** Same ids as the plan's columns, with the names and qualifiers SQL sees. */
  scope: Field[];
}

type Ctes = ReadonlyMap<string, { cte: Cte; outer: Ctes }>;

/** How a bound expression may use aggregates. */
type AggMode = "forbidden" | "allowed";

class Analyzer {
  private readonly catalog: CatalogReader;
  private nextId = 0;
  private readonly metas = new Map<string, TableMeta>();
  private readonly fileCache = new Map<string, DataFile[]>();
  /** Where each bound expression came from, for errors found later. */
  private readonly spans = new WeakMap<Expr, Span>();

  constructor(catalog: CatalogReader) {
    this.catalog = catalog;
  }

  private newId(): number {
    return ++this.nextId;
  }

  private fail(message: string, span: Span | null | undefined): never {
    throw new AnalysisError(message, span ?? null);
  }

  // --- queries ---------------------------------------------------------------------

  query(q: Query, outer: Ctes): Relation {
    let ctes: Ctes = outer;
    for (const cte of q.ctes) {
      const next = new Map(ctes);
      if (next.has(cte.name) && q.ctes.filter((c) => c.name === cte.name).length > 1) {
        this.fail(`CTE '${cte.name}' is defined twice`, cte.span);
      }
      next.set(cte.name, { cte, outer: ctes }); // a CTE sees the ones before it
      ctes = next;
    }
    return this.select(q.body, ctes);
  }

  private select(s: Select, ctes: Ctes): Relation {
    // FROM
    let rel: Relation = s.from ? this.from(s.from, ctes) : this.noFrom();
    const scope = rel.scope;
    if (s.from) this.checkAliases(s.from);

    // WHERE
    if (s.where) {
      const cond = this.predicate(s.where, scope, "WHERE");
      rel = { plan: new Filter(rel.plan, cond), scope };
    }

    // SELECT items, bound against the FROM scope (aggregates allowed for now)
    const items = this.selectItems(s.items, scope);
    // HAVING may name a select alias (`having total > 10`), as GROUP BY and ORDER BY
    // may; a column of the FROM tables with the same name wins.
    let having: Expr | null = null;
    if (s.having) {
      const aliases = new Map<string, Expr | null>();
      for (const it of items) if (it.alias !== null) aliases.set(it.alias, aliases.has(it.alias) ? null : unalias(it.expr));
      this.selectAliases = aliases;
      try {
        having = this.bind(s.having, scope, "allowed", "HAVING");
      } finally {
        this.selectAliases = null;
      }
    }
    if (having) this.checkBool(having, "HAVING", s.having!.span);

    // ORDER BY: an output column (by position or name), or an expression
    type OrderSpec = { output: number; ascending: boolean; nullsFirst: boolean } | { expr: Expr; ascending: boolean; nullsFirst: boolean; span: Span };
    const orders: OrderSpec[] = s.orderBy.map((o) => {
      const dir = { ascending: o.ascending, nullsFirst: o.nullsFirst ?? o.ascending };
      const e = o.expr;
      if (e.kind === "literal" && e.type === "int") {
        const k = e.value as number;
        if (k < 1 || k > items.length) this.fail(`ORDER BY position ${k} is not in the select list (1 to ${items.length})`, e.span);
        return { output: k - 1, ...dir };
      }
      if (e.kind === "column" && e.table === null) {
        const hits = items.flatMap((it, i) => (it.name === e.name && (it.alias !== null || !this.inScope(e.name, scope)) ? [i] : []));
        if (hits.length > 1) this.fail(`ORDER BY ${e.name} is ambiguous: the select list has ${hits.length} columns named so`, e.span);
        if (hits.length === 1) return { output: hits[0]!, ...dir };
      }
      return { expr: this.bind(e, scope, "allowed"), span: o.span, ...dir };
    });

    // GROUP BY and aggregates
    const usesAgg = items.some((it) => containsAgg(it.expr)) || (having !== null && containsAgg(having))
      || orders.some((o) => "expr" in o && containsAgg(o.expr));
    if (s.groupBy.length || usesAgg) {
      const keys = s.groupBy.map((g) => this.groupKey(g, items, scope));
      const aggs = new Map<string, AggCall>();
      const collect = (e: Expr) => {
        for (const x of walk(e)) if (x instanceof AggCall && !aggs.has(String(x))) aggs.set(String(x), x);
      };
      items.forEach((it) => collect(it.expr));
      if (having) collect(having);
      orders.forEach((o) => "expr" in o && collect(o.expr));

      const keyFields = keys.map((k) => this.outputField(k, k instanceof ColumnRef ? this.fieldOf(k, scope) : null));
      const aggList = [...aggs.values()];
      const aggFields = aggList.map((a) => new Field(null, a.display(), this.newId(), a.type));
      const agg = new Aggregate(rel.plan, keys, aggList, [...keyFields, ...aggFields]);
      const byText = new Map<string, Field>();
      keys.forEach((k, i) => byText.set(String(k), keyFields[i]!));
      aggList.forEach((a, i) => byText.set(String(a), aggFields[i]!));
      const lift = (e: Expr): Expr => rewriteTopDown(e, (x) => {
        const f = byText.get(String(x));
        if (f) return x instanceof ColumnRef ? new ColumnRef(f.id, x.name, f.type) : f.ref();
        if (x instanceof ColumnRef) {
          this.fail(`column '${x.name}' must appear in GROUP BY or be used in an aggregate function`, this.spans.get(x));
        }
        return undefined;
      });
      items.forEach((it) => (it.expr = lift(it.expr)));
      orders.forEach((o) => "expr" in o && (o.expr = lift(o.expr)));
      const aggScope = [...keyFields.map((f, i) => (keys[i] instanceof ColumnRef ? this.fieldOf(keys[i] as ColumnRef, scope) ?? f : f)), ...aggFields];
      rel = { plan: agg, scope: aggScope };
      if (having) rel = { plan: new Filter(rel.plan, lift(having)), scope: rel.scope };
    } else if (having) {
      this.fail("HAVING needs GROUP BY or an aggregate", s.having!.span);
    }

    // SELECT: the projection, plus hidden columns ORDER BY needs
    const exprs: Expr[] = [];
    const fields: Field[] = [];
    const used = new Set<number>();
    for (const it of items) {
      let e = it.expr;
      let id: number;
      if (e instanceof ColumnRef && it.alias === null && !used.has(e.id)) {
        id = e.id;
      } else {
        id = this.newId();
        if (!(e instanceof Alias)) e = new Alias(e, it.name);
      }
      used.add(id);
      exprs.push(e);
      fields.push(new Field(it.qualifier, it.name, id, it.expr.type));
    }
    const visible = fields.length;
    const keys: SortKey[] = [];
    for (const o of orders) {
      if ("output" in o) {
        keys.push(new SortKey(fields[o.output]!.ref(), o.ascending, o.nullsFirst));
        continue;
      }
      const text = String(o.expr);
      const i = items.findIndex((it) => String(it.expr) === text);
      if (i >= 0) {
        keys.push(new SortKey(fields[i]!.ref(), o.ascending, o.nullsFirst));
        continue;
      }
      if (s.distinct) this.fail("with SELECT DISTINCT, ORDER BY expressions must appear in the select list", o.span);
      const hidden = o.expr instanceof ColumnRef && !used.has(o.expr.id)
        ? new Field(null, o.expr.name, o.expr.id, o.expr.type)
        : new Field(null, o.expr.display(), this.newId(), o.expr.type);
      used.add(hidden.id);
      exprs.push(hidden.id === (o.expr as ColumnRef).id ? o.expr : new Alias(o.expr, hidden.name));
      fields.push(hidden);
      keys.push(new SortKey(hidden.ref(), o.ascending, o.nullsFirst));
    }
    let plan: PlanNode = new Project(rel.plan, exprs, fields);

    if (s.distinct) {
      const refs = fields.map((f) => f.ref());
      plan = new Aggregate(plan, refs, [], fields);
    }
    if (keys.length) plan = new Sort(plan, keys);
    if (s.limit) {
      const l = s.limit;
      if (l.kind !== "literal" || l.type !== "int" || (l.value as number) < 0) this.fail("LIMIT needs a non-negative int", l.span);
      plan = new Limit(plan, l.value as number);
    }
    if (fields.length > visible) {
      const shown = fields.slice(0, visible);
      plan = new Project(plan, shown.map((f) => f.ref()), shown);
    }
    return { plan, scope: plan.schema.map((f) => f) };
  }

  private noFrom(): Relation {
    this.fail("a query needs FROM", null);
  }

  private inScope(name: string, scope: readonly Field[]): boolean {
    return scope.some((f) => f.name === name);
  }

  /** The scope field an id has, for qualifiers of passed-through columns. */
  private fieldOf(ref: ColumnRef, scope: readonly Field[]): Field | null {
    return scope.find((f) => f.id === ref.id) ?? null;
  }

  private outputField(e: Expr, from: Field | null): Field {
    if (e instanceof ColumnRef) return new Field(from?.qualifier ?? null, e.name, e.id, e.type);
    return new Field(null, e.display(), this.newId(), e.type);
  }

  private groupKey(g: AstExpr, items: SelectItemBound[], scope: readonly Field[]): Expr {
    if (g.kind === "literal" && g.type === "int") {
      const k = g.value as number;
      if (k < 1 || k > items.length) this.fail(`GROUP BY position ${k} is not in the select list (1 to ${items.length})`, g.span);
      const e = items[k - 1]!.expr;
      if (containsAgg(e)) this.fail(`GROUP BY ${k} refers to an aggregate`, g.span);
      return unalias(e);
    }
    if (g.kind === "column" && g.table === null && !this.inScope(g.name, scope)) {
      const hit = items.find((it) => it.alias === g.name);
      if (hit) {
        if (containsAgg(hit.expr)) this.fail(`GROUP BY ${g.name} refers to an aggregate`, g.span);
        return unalias(hit.expr);
      }
    }
    return this.bind(g, scope, "forbidden", "GROUP BY");
  }

  private selectItems(list: readonly SelectItem[], scope: readonly Field[]): SelectItemBound[] {
    const out: SelectItemBound[] = [];
    for (const it of list) {
      if (it.expr === null) {
        const cols = it.star === null ? scope : scope.filter((f) => f.qualifier === it.star);
        if (it.star && !cols.length) this.fail(`no table named '${it.star}' in FROM`, it.span);
        for (const f of cols) out.push({ expr: f.ref(), alias: null, name: f.name, qualifier: f.qualifier });
        continue;
      }
      const e = this.bind(it.expr, scope, "allowed");
      const ref = e instanceof ColumnRef ? this.fieldOf(e, scope) : null;
      out.push({
        expr: e, alias: it.alias, name: it.alias ?? (e instanceof ColumnRef ? e.name : outerParensOff(e.display())),
        qualifier: it.alias === null ? ref?.qualifier ?? null : null,
      });
    }
    return out;
  }

  // --- FROM ------------------------------------------------------------------------

  private checkAliases(from: FromItem): void {
    const seen = new Map<string, FromItem>();
    const visit = (f: FromItem) => {
      if (f.kind === "join") {
        visit(f.left);
        visit(f.right);
        return;
      }
      const name = f.kind === "table" ? f.alias ?? f.name : f.alias;
      if (seen.has(name)) {
        this.fail(`'${name}' appears twice in FROM; give one of them an alias (FROM t AS a JOIN t AS b ...)`, f.span);
      }
      seen.set(name, f);
    };
    visit(from);
  }

  private from(f: FromItem, ctes: Ctes): Relation {
    switch (f.kind) {
      case "table": {
        const cte = ctes.get(f.name);
        if (cte && !f.asOf) {
          const rel = this.query(cte.cte.query, cte.outer);
          const names = cte.cte.columns;
          if (names && names.length !== rel.scope.length) {
            this.fail(`CTE '${f.name}' names ${names.length} columns but its query has ${rel.scope.length}`, cte.cte.span);
          }
          const q = f.alias ?? f.name;
          return { plan: rel.plan, scope: rel.scope.map((x, i) => new Field(q, names?.[i] ?? x.name, x.id, x.type)) };
        }
        return this.scan(f);
      }
      case "subquery": {
        const rel = this.query(f.query, ctes);
        return { plan: rel.plan, scope: rel.scope.map((x) => x.withQualifier(f.alias)) };
      }
      case "join": {
        if (f.how === "full") this.fail("FULL JOIN is not supported", f.span);
        const left = this.from(f.left, ctes);
        const right = this.from(f.right, ctes);
        const scope = [...left.scope, ...right.scope];
        if (f.how === "cross") return { plan: new Join(left.plan, right.plan, "cross", null), scope };
        const cond = this.predicate(f.on!, scope, "ON");
        if (f.how === "right") {
          // A right join is a left join with the sides swapped, columns put back in order.
          const swapped = new Join(right.plan, left.plan, "left", cond);
          const fields = [...left.plan.schema, ...right.plan.schema];
          return { plan: new Project(swapped, fields.map((x) => x.ref()), fields), scope };
        }
        return { plan: new Join(left.plan, right.plan, f.how, cond), scope };
      }
    }
  }

  private table(name: string, span: Span): TableMeta {
    let meta = this.metas.get(name);
    if (!meta) {
      try {
        meta = this.catalog.loadTable(name);
      } catch (e) {
        if (e instanceof TableNotFoundError) this.fail(`unknown table '${name}'`, span);
        throw e;
      }
      this.metas.set(name, meta);
    }
    return meta;
  }

  private scan(f: Extract<FromItem, { kind: "table" }>): Relation {
    const meta = this.table(f.name, f.span);
    let snapshot = meta.current;
    if (f.asOf) {
      const v = f.asOf.value;
      if (f.asOf.kind === "version") {
        if (v.kind !== "literal" || v.type !== "int") this.fail("FOR VERSION AS OF needs an int", v.span);
        const s = meta.snapshot(v.value as number);
        if (!s) {
          this.fail(`table '${f.name}' has no version ${v.value}; versions: ${meta.snapshots.map((x) => x.version).join(", ")}`, v.span);
        }
        snapshot = s!;
      } else {
        if (v.kind !== "literal" || !["string", "timestamp", "date"].includes(v.type)) {
          this.fail("FOR TIMESTAMP AS OF needs a timestamp, e.g. TIMESTAMP '2026-01-01 12:00'", v.span);
        }
        const us = parseTimestamp(v.value as string);
        if (us === null) this.fail(`'${v.value}' is not a timestamp`, v.span);
        const s = meta.snapshotAsOf(us!);
        if (!s) this.fail(`table '${f.name}' did not exist yet at ${v.value}`, v.span);
        snapshot = s!;
      }
    }
    const key = `${meta.name}@${snapshot.version}`;
    let files = this.fileCache.get(key);
    if (!files) {
      files = this.catalog.files(meta, snapshot.version);
      this.fileCache.set(key, files);
    }
    const q = f.alias ?? f.name;
    const schema = snapshot.columns.map((c) => new Field(q, c.name, this.newId(), c.type));
    const plan = new Scan({
      table: meta.name, meta, version: snapshot.version, alias: f.alias,
      columns: snapshot.columns.map((c) => c.name), types: snapshot.columns.map((c) => c.type),
      files, snapshotFiles: files,
    }, schema);
    return { plan, scope: schema };
  }

  // --- expressions -------------------------------------------------------------

  private predicate(e: AstExpr, scope: readonly Field[], where: string): Expr {
    const bound = this.bind(e, scope, "forbidden", where);
    this.checkBool(bound, where, e.span);
    return bound;
  }

  private checkBool(e: Expr, where: string, span: Span): void {
    if (e.type !== "bool" && e.type !== "null") this.fail(`${where} needs a bool condition, got ${e.type}`, span);
  }

  private resolveColumn(e: Extract<AstExpr, { kind: "column" }>, scope: readonly Field[]): Field {
    const hits = scope.filter((f) => f.name === e.name && (e.table === null || f.qualifier === e.table));
    const shown = e.table ? `${e.table}.${e.name}` : e.name;
    if (!hits.length) {
      if (e.table && !scope.some((f) => f.qualifier === e.table)) this.fail(`unknown table or alias '${e.table}' in '${shown}'`, e.span);
      const close = scope.filter((f) => f.name.toLowerCase() === e.name.toLowerCase());
      const hint = close.length ? `; did you mean ${close.map(String).join(" or ")}?` : `; available: ${scope.map(String).join(", ") || "(none)"}`;
      this.fail(`unknown column '${shown}'${hint}`, e.span);
    }
    if (hits.length > 1) {
      this.fail(`column '${shown}' is ambiguous (${hits.map(String).join(", ")}); qualify it as table.column`, e.span);
    }
    return hits[0]!;
  }

  /** While binding HAVING: the select list's aliases (null: used twice). */
  private selectAliases: Map<string, Expr | null> | null = null;

  bind(e: AstExpr, scope: readonly Field[], aggs: AggMode, where = "here"): Expr {
    const out = this.bindInner(e, scope, aggs, where);
    if (!this.spans.has(out)) this.spans.set(out, e.span);
    return out;
  }

  private bindInner(e: AstExpr, scope: readonly Field[], aggs: AggMode, where: string): Expr {
    const sub = (x: AstExpr, mode: AggMode = aggs) => this.bind(x, scope, mode, where);
    switch (e.kind) {
      case "column": {
        if (e.table === null && this.selectAliases?.has(e.name) && !this.inScope(e.name, scope)) {
          const x = this.selectAliases.get(e.name);
          if (!x) this.fail(`${where} ${e.name} is ambiguous: the select list has several columns named so`, e.span);
          return x!;
        }
        const f = this.resolveColumn(e, scope);
        return new ColumnRef(f.id, f.name, f.type);
      }
      case "literal":
        return this.literal(e);
      case "op": {
        const args = this.coerceLiterals(e.op, e.args.map((a) => sub(a)), e.args);
        return this.callNamed(e.op, args, e.span);
      }
      case "in": {
        const args = this.coerceLiterals("in", [sub(e.expr), ...e.list.map((a) => sub(a))], [e.expr, ...e.list]);
        const c = this.callNamed("in", args, e.span);
        return e.negated ? this.callNamed("not", [c], e.span) : c;
      }
      case "between": {
        const [x, lo, hi] = this.coerceLiterals("between", [sub(e.expr), sub(e.low), sub(e.high)], [e.expr, e.low, e.high]);
        const c = this.callNamed("and", [this.callNamed(">=", [x!, lo!], e.span), this.callNamed("<=", [x!, hi!], e.span)], e.span);
        return e.negated ? this.callNamed("not", [c], e.span) : c;
      }
      case "cast": {
        let t: ColumnType;
        try {
          t = parseType(e.type);
        } catch {
          this.fail(`unknown type '${e.type}'`, e.span);
        }
        const child = sub(e.expr);
        if (!castImpl(child.type, t)) this.fail(`cannot cast ${child.type} to ${t}`, e.span);
        if (child instanceof Literal) {
          try {
            return new Literal(castValue(child.value, child.type, t), t);
          } catch (err) {
            if (err instanceof ExecutionError) this.fail(err.message, e.span);
            throw err;
          }
        }
        return new Cast(child, t);
      }
      case "case":
        return this.caseExpr(e, scope, aggs, where);
      case "call":
        return this.callExpr(e, scope, aggs, where);
    }
  }

  private literal(e: Extract<AstExpr, { kind: "literal" }>): Literal {
    switch (e.type) {
      case "date": {
        const d = parseDate(e.value as string);
        if (d === null || !/^\s*\d{4}-\d{2}-\d{2}\s*$/.test(e.value as string)) this.fail(`'${e.value}' is not a date (YYYY-MM-DD)`, e.span);
        return new Literal(d, "date");
      }
      case "timestamp": {
        const t = parseTimestamp(e.value as string);
        if (t === null) this.fail(`'${e.value}' is not a timestamp`, e.span);
        return new Literal(t, "timestamp");
      }
      default:
        return new Literal(e.value, e.type);
    }
  }

  /** A string literal next to a date or timestamp is read as one (`d < '2024-01-01'`). */
  private coerceLiterals(op: string, args: Expr[], ast: readonly AstExpr[]): Expr[] {
    if (!["=", "<>", "<", "<=", ">", ">=", "in", "between"].includes(op)) return args;
    const temporal = args.find((a) => isTemporal(a.type));
    if (!temporal) return args;
    return args.map((a, i) => {
      if (!(a instanceof Literal) || a.type !== "string" || a.value === null) return a;
      const t = temporal.type === "date" && parseDate(a.value as string) !== null && /^\s*\d{4}-\d{2}-\d{2}\s*$/.test(a.value as string)
        ? "date" : "timestamp";
      const v = t === "date" ? parseDate(a.value as string) : parseTimestamp(a.value as string);
      if (v === null) this.fail(`'${a.value}' is not a ${temporal.type}`, ast[i]!.span);
      return new Literal(v, t);
    });
  }

  private callNamed(name: string, args: readonly Expr[], span: Span): Expr {
    try {
      return call(name, args);
    } catch (err) {
      if (err instanceof TypeMismatch) this.fail(err.message, span);
      throw err;
    }
  }

  private caseExpr(e: Extract<AstExpr, { kind: "case" }>, scope: readonly Field[], aggs: AggMode, where: string): Expr {
    const sub = (x: AstExpr) => this.bind(x, scope, aggs, where);
    const operand = e.operand ? sub(e.operand) : null;
    const whens = e.whens.map(([c, v]) => {
      let cond = sub(c);
      if (operand) cond = this.callNamed("=", this.coerceLiterals("=", [operand, cond], [e.operand!, c]), c.span);
      else this.checkBool(cond, "CASE WHEN", c.span);
      return [cond, sub(v)] as const;
    });
    const otherwise = e.otherwise ? sub(e.otherwise) : new Literal(null, "null");
    const values = [...whens.map(([, v]) => v), otherwise];
    const t = commonType(values.map((v) => v.type));
    if (t === null) this.fail(`CASE branches have different types: ${values.map((v) => v.type).join(", ")}`, e.span);
    return new Case(whens.map(([c, v]) => [c, coerce(v, t!)] as const), coerce(otherwise, t!), t!);
  }

  private callExpr(e: Extract<AstExpr, { kind: "call" }>, scope: readonly Field[], aggs: AggMode, where: string): Expr {
    const agg = lookupAggregate(e.name);
    if (agg) {
      if (aggs === "forbidden") this.fail(`aggregate ${e.name}() is not allowed in ${where}`, e.span);
      if (e.distinct) this.fail(`${e.name}(DISTINCT ...) is not supported; use a subquery with SELECT DISTINCT`, e.span);
      if (e.star && e.name !== "count") this.fail(`${e.name}(*) is not an aggregate; only count(*) is`, e.span);
      if (!e.star && e.args.length !== 1) this.fail(`${e.name}() takes one argument`, e.span);
      const arg = e.star ? null : this.bind(e.args[0]!, scope, "forbidden", `the argument of ${e.name}()`);
      try {
        return new AggCall(agg, arg, agg.resolve(arg?.type ?? null));
      } catch (err) {
        if (err instanceof TypeMismatch) this.fail(err.message, e.span);
        throw err;
      }
    }
    const fn = lookupFunction(e.name);
    if (!fn || fn.operator) {
      const names = [...allFunctions().filter((f) => !f.operator).map((f) => f.name), ...allAggregates().map((a) => a.name)];
      const close = names.filter((n) => n.includes(e.name) || e.name.includes(n)).slice(0, 3);
      this.fail(`unknown function '${e.name}'${close.length ? `; did you mean ${close.join(", ")}?` : ""}`, e.span);
    }
    if (e.star || e.distinct) this.fail(`${e.name}() is not an aggregate`, e.span);
    const args = e.args.map((a) => this.bind(a, scope, aggs, where));
    return this.callNamed(e.name, args, e.span);
  }
}

interface SelectItemBound {
  expr: Expr;
  alias: string | null;
  name: string;
  qualifier: string | null;
}

/** "(a + b)" -> "a + b", but "(a) + (b)" stays: a column name for an expression. */
function outerParensOff(text: string): string {
  if (!text.startsWith("(") || !text.endsWith(")")) return text;
  let depth = 0, quoted = false;
  for (let i = 0; i < text.length - 1; i++) {
    const ch = text[i];
    if (ch === "'") quoted = !quoted;
    else if (!quoted && ch === "(") depth++;
    else if (!quoted && ch === ")" && --depth === 0) return text;
  }
  return text.slice(1, -1);
}
