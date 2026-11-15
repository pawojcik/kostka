// A recursive-descent parser for kostka's SQL: one SELECT statement, optionally with
// CTEs. Grammar (keywords case-insensitive):
//
//   query     := [WITH name [(cols)] AS (query), ...] select
//   select    := SELECT [DISTINCT] item, ... [FROM from] [WHERE expr]
//                [GROUP BY expr, ...] [HAVING expr] [ORDER BY expr [ASC|DESC]
//                [NULLS FIRST|LAST], ...] [LIMIT int]
//   item      := * | name.* | expr [[AS] alias]
//   from      := source {, source | [INNER|LEFT [OUTER]|RIGHT [OUTER]|CROSS] JOIN source [ON expr]}
//   source    := table [FOR VERSION AS OF int | FOR TIMESTAMP AS OF expr] [[AS] alias]
//              | (query) [AS] alias | (from)
//   expr      := the usual precedence: OR < AND < NOT < comparison (= <> < <= > >=,
//                IS [NOT] NULL, [NOT] IN (...), [NOT] BETWEEN, [NOT] LIKE)
//                < + - || < * / % < unary - < primary
//   primary   := literal | DATE 'yyyy-mm-dd' | TIMESTAMP '...' | column | t.column
//              | fn(args) | count(*) | CAST(expr AS type) | CASE ... END | (expr)

import { ParseError, type Span } from "../core/errors.ts";
import type { AsOf, AstExpr, Cte, FromItem, OrderItem, Query, Select, SelectItem } from "./ast.ts";
import { tokenize, type Token, type TokenKind } from "./lexer.ts";

export function parse(sql: string): Query {
  const p = new Parser(tokenize(sql));
  const q = p.query();
  p.accept("op", ";");
  if (p.peek().kind !== "eof") p.fail(`unexpected ${p.describe(p.peek())} after the end of the query`);
  return q;
}

const span = (a: Span, b: Span): Span => [a[0], b[1]];

/** Keywords that end a clause: never taken as an implicit alias. */
const CLAUSE_WORDS = new Set(["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "JOIN", "INNER", "LEFT",
  "RIGHT", "FULL", "CROSS", "ON", "FOR", "UNION"]);

class Parser {
  private pos = 0;
  private readonly tokens: Token[];
  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  peek(offset = 0): Token {
    return this.tokens[Math.min(this.pos + offset, this.tokens.length - 1)]!;
  }
  private next(): Token {
    const t = this.peek();
    if (t.kind !== "eof") this.pos++;
    return t;
  }
  private last(): Token {
    return this.tokens[Math.max(0, this.pos - 1)]!;
  }
  private is(kind: TokenKind, value?: string, offset = 0): boolean {
    const t = this.peek(offset);
    return t.kind === kind && (value === undefined || t.value === value);
  }
  private isKw(...words: string[]): boolean {
    return this.peek().kind === "keyword" && words.includes(this.peek().value);
  }
  accept(kind: TokenKind, value?: string): Token | null {
    return this.is(kind, value) ? this.next() : null;
  }
  private kw(word: string): Token | null {
    return this.accept("keyword", word);
  }
  private expect(kind: TokenKind, value?: string, what?: string): Token {
    const t = this.accept(kind, value);
    if (!t) this.fail(`expected ${what ?? (value ? `'${value}'` : kind)}, got ${this.describe(this.peek())}`);
    return t!;
  }
  private expectKw(word: string): Token {
    return this.expect("keyword", word, word);
  }
  describe(t: Token): string {
    if (t.kind === "eof") return "the end of the query";
    if (t.kind === "string") return `'${t.value}'`;
    return t.kind === "keyword" ? t.value : `'${t.value}'`;
  }
  fail(message: string, at: Span = this.peek().span): never {
    throw new ParseError(message, at);
  }

  private name(what: string): string {
    const t = this.accept("ident") ?? this.accept("quoted");
    if (!t) this.fail(`expected ${what}, got ${this.describe(this.peek())}`);
    return t!.value;
  }

  // --- statements ----------------------------------------------------------------

  query(): Query {
    const start = this.peek().span;
    const ctes: Cte[] = [];
    if (this.kw("WITH")) {
      do {
        const s = this.peek().span;
        const name = this.name("a CTE name");
        let columns: string[] | null = null;
        if (this.accept("op", "(")) {
          columns = [];
          do columns.push(this.name("a column name")); while (this.accept("op", ","));
          this.expect("op", ")");
        }
        this.expectKw("AS");
        this.expect("op", "(");
        const query = this.query();
        this.expect("op", ")");
        ctes.push({ name, columns, query, span: span(s, this.last().span) });
      } while (this.accept("op", ","));
    }
    const body = this.select();
    return { ctes, body, span: span(start, this.last().span) };
  }

  private select(): Select {
    const start = this.expectKw("SELECT").span;
    const distinct = !!this.kw("DISTINCT");
    if (!distinct) this.kw("ALL");
    const items: SelectItem[] = [];
    do items.push(this.selectItem()); while (this.accept("op", ","));
    const from = this.kw("FROM") ? this.from() : null;
    const where = this.kw("WHERE") ? this.expr() : null;
    const groupBy: AstExpr[] = [];
    if (this.kw("GROUP")) {
      this.expectKw("BY");
      do groupBy.push(this.expr()); while (this.accept("op", ","));
    }
    const having = this.kw("HAVING") ? this.expr() : null;
    const orderBy: OrderItem[] = [];
    if (this.kw("ORDER")) {
      this.expectKw("BY");
      do orderBy.push(this.orderItem()); while (this.accept("op", ","));
    }
    let limit: AstExpr | null = null;
    if (this.kw("LIMIT")) limit = this.primary();
    if (this.isKw("UNION")) this.fail("UNION is not supported");
    return { distinct, items, from, where, groupBy, having, orderBy, limit, span: span(start, this.last().span) };
  }

  private selectItem(): SelectItem {
    const s = this.peek().span;
    if (this.accept("op", "*")) return { expr: null, star: null, alias: null, span: s };
    if ((this.is("ident") || this.is("quoted")) && this.is("op", ".", 1) && this.is("op", "*", 2)) {
      const q = this.next().value;
      this.next();
      this.next();
      return { expr: null, star: q, alias: null, span: span(s, this.last().span) };
    }
    const expr = this.expr();
    const alias = this.alias();
    return { expr, star: undefined, alias, span: span(s, this.last().span) };
  }

  private alias(): string | null {
    if (this.kw("AS")) return this.name("an alias");
    const t = this.peek();
    if (t.kind === "ident" || t.kind === "quoted") return this.next().value;
    return null;
  }

  private orderItem(): OrderItem {
    const s = this.peek().span;
    const expr = this.expr();
    let ascending = true;
    if (this.kw("DESC")) ascending = false;
    else this.kw("ASC");
    let nullsFirst: boolean | null = null;
    if (this.kw("NULLS")) {
      if (this.kw("FIRST")) nullsFirst = true;
      else if (this.kw("LAST")) nullsFirst = false;
      else this.fail("expected FIRST or LAST after NULLS");
    }
    return { expr, ascending, nullsFirst, span: span(s, this.last().span) };
  }

  // --- FROM ------------------------------------------------------------------------

  private from(): FromItem {
    let left = this.joined();
    while (this.accept("op", ",")) {
      const right = this.joined();
      left = { kind: "join", how: "cross", left, right, on: null, span: span(left.span, right.span) };
    }
    return left;
  }

  private joined(): FromItem {
    let left = this.source();
    for (;;) {
      let how: "inner" | "left" | "right" | "full" | "cross";
      const s = this.peek().span;
      if (this.kw("JOIN")) how = "inner";
      else if (this.kw("INNER")) {
        this.expectKw("JOIN");
        how = "inner";
      } else if (this.isKw("LEFT", "RIGHT", "FULL")) {
        how = this.next().value.toLowerCase() as "left" | "right" | "full";
        this.kw("OUTER");
        this.expectKw("JOIN");
      } else if (this.kw("CROSS")) {
        this.expectKw("JOIN");
        how = "cross";
      } else return left;
      const right = this.source();
      let on: AstExpr | null = null;
      if (how !== "cross") {
        if (!this.kw("ON")) this.fail(`expected ON after ${how.toUpperCase()} JOIN ... (or use CROSS JOIN)`, s);
        on = this.expr();
      }
      left = { kind: "join", how, left, right, on, span: span(left.span, this.last().span) };
    }
  }

  private source(): FromItem {
    const s = this.peek().span;
    if (this.accept("op", "(")) {
      if (this.isKw("SELECT", "WITH")) {
        const query = this.query();
        this.expect("op", ")");
        const alias = this.alias();
        if (!alias) this.fail("a subquery in FROM needs an alias: (SELECT ...) AS name");
        return { kind: "subquery", query, alias: alias!, span: span(s, this.last().span) };
      }
      const inner = this.from();
      this.expect("op", ")");
      return inner;
    }
    const name = this.name("a table name");
    let asOf: AsOf | null = null;
    if (this.kw("FOR")) {
      if (this.kw("VERSION")) {
        this.expectKw("AS");
        this.expectKw("OF");
        asOf = { kind: "version", value: this.primary() };
      } else if (this.kw("TIMESTAMP")) {
        this.expectKw("AS");
        this.expectKw("OF");
        asOf = { kind: "timestamp", value: this.primary() };
      } else this.fail("expected VERSION or TIMESTAMP after FOR");
    }
    const alias = this.isKw(...CLAUSE_WORDS) ? null : this.alias();
    return { kind: "table", name, alias, asOf, span: span(s, this.last().span) };
  }

  // --- expressions ---------------------------------------------------------------

  expr(): AstExpr {
    return this.or();
  }

  private binary(next: () => AstExpr, ops: Record<string, string>, keyword: boolean): AstExpr {
    let left = next();
    for (;;) {
      const t = this.peek();
      const op = (keyword ? t.kind === "keyword" : t.kind === "op") ? ops[t.value] : undefined;
      if (!op) return left;
      this.next();
      const right = next();
      left = { kind: "op", op, args: [left, right], span: span(left.span, right.span) };
    }
  }

  private or(): AstExpr {
    return this.binary(() => this.and(), { OR: "or" }, true);
  }
  private and(): AstExpr {
    return this.binary(() => this.not(), { AND: "and" }, true);
  }
  private not(): AstExpr {
    const t = this.kw("NOT");
    if (!t) return this.comparison();
    const e = this.not();
    return { kind: "op", op: "not", args: [e], span: span(t.span, e.span) };
  }

  private comparison(): AstExpr {
    const left = this.additive();
    const t = this.peek();
    if (t.kind === "op" && ["=", "<>", "<", "<=", ">", ">="].includes(t.value)) {
      this.next();
      const right = this.additive();
      return { kind: "op", op: t.value, args: [left, right], span: span(left.span, right.span) };
    }
    if (this.kw("IS")) {
      const negated = !!this.kw("NOT");
      this.expectKw("NULL");
      return { kind: "op", op: negated ? "is_not_null" : "is_null", args: [left], span: span(left.span, this.last().span) };
    }
    const negated = this.isKw("NOT") && this.peek(1).kind === "keyword" && ["IN", "BETWEEN", "LIKE"].includes(this.peek(1).value);
    if (negated) this.next();
    if (this.kw("IN")) {
      this.expect("op", "(");
      if (this.isKw("SELECT")) this.fail("IN (subquery) is not supported; use a join");
      const list: AstExpr[] = [];
      do list.push(this.expr()); while (this.accept("op", ","));
      this.expect("op", ")");
      return { kind: "in", expr: left, list, negated, span: span(left.span, this.last().span) };
    }
    if (this.kw("BETWEEN")) {
      const low = this.additive();
      this.expectKw("AND");
      const high = this.additive();
      return { kind: "between", expr: left, low, high, negated, span: span(left.span, high.span) };
    }
    if (this.kw("LIKE")) {
      const pattern = this.additive();
      const like: AstExpr = { kind: "op", op: "like", args: [left, pattern], span: span(left.span, pattern.span) };
      return negated ? { kind: "op", op: "not", args: [like], span: like.span } : like;
    }
    if (negated) this.fail("expected IN, BETWEEN or LIKE after NOT");
    return left;
  }

  private additive(): AstExpr {
    return this.binary(() => this.multiplicative(), { "+": "+", "-": "-", "||": "concat" }, false);
  }
  private multiplicative(): AstExpr {
    return this.binary(() => this.unary(), { "*": "*", "/": "/", "%": "%" }, false);
  }
  private unary(): AstExpr {
    const t = this.accept("op", "-") ?? this.accept("op", "+");
    if (!t) return this.primary();
    const e = this.unary();
    if (t.value === "+") return e;
    if (e.kind === "literal" && (e.type === "int" || e.type === "float")) {
      return { ...e, value: -(e.value as number) || 0, text: `-${e.text}`, span: span(t.span, e.span) };
    }
    return { kind: "op", op: "neg", args: [e], span: span(t.span, e.span) };
  }

  primary(): AstExpr {
    const t = this.peek();
    switch (t.kind) {
      case "int":
      case "float": {
        this.next();
        const value = Number(t.value);
        if (t.kind === "int" && !Number.isSafeInteger(value)) this.fail(`int literal ${t.value} is out of range (±2^53)`, t.span);
        return { kind: "literal", value, type: t.kind, text: t.value, span: t.span };
      }
      case "string":
        this.next();
        return { kind: "literal", value: t.value, type: "string", text: t.value, span: t.span };
      case "op":
        if (t.value === "(") {
          this.next();
          if (this.isKw("SELECT")) this.fail("scalar subqueries are not supported; use a join");
          const e = this.expr();
          this.expect("op", ")");
          return { ...e, span: span(t.span, this.last().span) };
        }
        break;
      case "keyword":
        return this.keywordPrimary(t);
      case "ident":
      case "quoted": {
        this.next();
        if (t.kind === "ident" && this.is("op", "(")) return this.call(t);
        if (this.accept("op", ".")) {
          const col = this.accept("ident") ?? this.accept("quoted");
          if (!col) this.fail(`expected a column name after '${t.value}.'`);
          return { kind: "column", table: t.value, name: col!.value, span: span(t.span, col!.span) };
        }
        return { kind: "column", table: null, name: t.value, span: t.span };
      }
    }
    this.fail(`expected an expression, got ${this.describe(t)}`);
  }

  private keywordPrimary(t: Token): AstExpr {
    switch (t.value) {
      case "NULL":
        this.next();
        return { kind: "literal", value: null, type: "null", text: "NULL", span: t.span };
      case "TRUE":
      case "FALSE":
        this.next();
        return { kind: "literal", value: t.value === "TRUE", type: "bool", text: t.value, span: t.span };
      case "DATE":
      case "TIMESTAMP": {
        this.next();
        const s = this.accept("string");
        if (!s) {
          if (this.is("op", "(")) return this.call({ ...t, value: t.value.toLowerCase() });
          this.fail(`expected a string after ${t.value}, e.g. ${t.value} '2024-05-17'`);
        }
        return { kind: "literal", value: s!.value, type: t.value === "DATE" ? "date" : "timestamp", text: s!.value, span: span(t.span, s!.span) };
      }
      case "CAST": {
        this.next();
        this.expect("op", "(");
        const expr = this.expr();
        this.expectKw("AS");
        const type = this.accept("ident") ?? this.accept("keyword");
        if (!type) this.fail("expected a type name");
        this.expect("op", ")");
        return { kind: "cast", expr, type: type!.value, span: span(t.span, this.last().span) };
      }
      case "CASE":
        return this.caseExpr();
      case "LEFT":
      case "RIGHT":
        break;
    }
    this.fail(`expected an expression, got ${t.value}`);
  }

  private call(nameToken: Token): AstExpr {
    this.expect("op", "(");
    const name = nameToken.value.toLowerCase();
    if (this.accept("op", "*")) {
      this.expect("op", ")");
      return { kind: "call", name, args: [], star: true, distinct: false, span: span(nameToken.span, this.last().span) };
    }
    const distinct = !!this.kw("DISTINCT");
    const args: AstExpr[] = [];
    if (!this.is("op", ")")) do args.push(this.expr()); while (this.accept("op", ","));
    this.expect("op", ")");
    return { kind: "call", name, args, star: false, distinct, span: span(nameToken.span, this.last().span) };
  }

  private caseExpr(): AstExpr {
    const start = this.next().span;
    const operand = this.isKw("WHEN") ? null : this.expr();
    const whens: [AstExpr, AstExpr][] = [];
    while (this.kw("WHEN")) {
      const cond = this.expr();
      this.expectKw("THEN");
      whens.push([cond, this.expr()]);
    }
    if (!whens.length) this.fail("CASE needs at least one WHEN ... THEN ...");
    const otherwise = this.kw("ELSE") ? this.expr() : null;
    this.expectKw("END");
    return { kind: "case", operand, whens, otherwise, span: span(start, this.last().span) };
  }
}
