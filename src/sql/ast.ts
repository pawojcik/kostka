// The syntax tree of a query, as parsed. Nothing is resolved yet: names are strings
// and every node carries its `span` in the SQL text, for error messages.

import type { Span } from "../core/errors.ts";

export type AstExpr =
  | { kind: "column"; table: string | null; name: string; span: Span }
  | { kind: "literal"; value: null | boolean | number | string; type: "int" | "float" | "string" | "bool" | "null" | "date" | "timestamp"; text: string; span: Span }
  | { kind: "call"; name: string; args: AstExpr[]; star: boolean; distinct: boolean; span: Span }
  | { kind: "op"; op: string; args: AstExpr[]; span: Span }
  | { kind: "cast"; expr: AstExpr; type: string; span: Span }
  | { kind: "case"; operand: AstExpr | null; whens: [AstExpr, AstExpr][]; otherwise: AstExpr | null; span: Span }
  | { kind: "in"; expr: AstExpr; list: AstExpr[]; negated: boolean; span: Span }
  | { kind: "between"; expr: AstExpr; low: AstExpr; high: AstExpr; negated: boolean; span: Span };

export interface SelectItem {
  /** null for `*` or `t.*`. */
  expr: AstExpr | null;
  /** `t.*`: the qualifier; `*`: null. */
  star: string | null | undefined;
  alias: string | null;
  span: Span;
}

export type AsOf = { kind: "version"; value: AstExpr } | { kind: "timestamp"; value: AstExpr };

export type FromItem =
  | { kind: "table"; name: string; alias: string | null; asOf: AsOf | null; span: Span }
  | { kind: "subquery"; query: Query; alias: string; span: Span }
  | { kind: "join"; how: "inner" | "left" | "right" | "full" | "cross"; left: FromItem; right: FromItem; on: AstExpr | null; span: Span };

export interface OrderItem {
  expr: AstExpr;
  ascending: boolean;
  nullsFirst: boolean | null;
  span: Span;
}

export interface Select {
  distinct: boolean;
  items: SelectItem[];
  from: FromItem | null;
  where: AstExpr | null;
  groupBy: AstExpr[];
  having: AstExpr | null;
  orderBy: OrderItem[];
  limit: AstExpr | null;
  span: Span;
}

export interface Cte {
  name: string;
  columns: string[] | null;
  query: Query;
  span: Span;
}

export interface Query {
  ctes: Cte[];
  body: Select;
  span: Span;
}
