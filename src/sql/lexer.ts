// Splits SQL text into tokens. Keywords are upper-cased; identifiers are kept as
// written (the analyzer compares them ignoring case) and may be "double-quoted" to
// use a keyword as a name. Comments:
// `-- to the end of the line` and `/* ... */`.

import { ParseError, type Span } from "../core/errors.ts";

export type TokenKind = "ident" | "quoted" | "keyword" | "int" | "float" | "string" | "op" | "eof";

export interface Token {
  kind: TokenKind;
  /** Keywords upper-cased; identifiers as written; strings unescaped. */
  value: string;
  span: Span;
}

export const KEYWORDS = new Set([
  "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "AS", "ASC", "DESC",
  "NULLS", "FIRST", "LAST", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "AND", "OR",
  "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "BETWEEN", "LIKE", "CASE", "WHEN", "THEN", "ELSE", "END",
  "CAST", "WITH", "DATE", "TIMESTAMP", "FOR", "VERSION", "OF", "ALL",
]);

const SOFT_KEYWORDS = new Set(["DATE", "TIMESTAMP"]);

const OPERATORS = ["<>", "!=", "<=", ">=", "||", "=", "<", ">", "+", "-", "*", "/", "%", "(", ")", ",", ".", ";"];

export function tokenize(sql: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i]!;
    if (/\s/.test(c)) {
      i++;
      continue;
    }
    if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i++;
      continue;
    }
    if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      if (end < 0) throw new ParseError("unterminated comment", [i, n]);
      i = end + 2;
      continue;
    }
    const start = i;
    if (/[A-Za-z_]/.test(c)) {
      while (i < n && /[A-Za-z0-9_]/.test(sql[i]!)) i++;
      const word = sql.slice(start, i);
      const upper = word.toUpperCase();
      // DATE and TIMESTAMP are keywords only before a literal or a call, or after FOR (time
      // travel), so tables and columns can be called date (as in SSB) or timestamp.
      const prev = out[out.length - 1];
      const soft = SOFT_KEYWORDS.has(upper) && !/^\s*['(]/.test(sql.slice(i, i + 64)) && !(prev?.kind === "keyword" && prev.value === "FOR");
      out.push(KEYWORDS.has(upper) && !soft ? { kind: "keyword", value: upper, span: [start, i] } : { kind: "ident", value: word, span: [start, i] });
      continue;
    }
    if (/[0-9]/.test(c) || (c === "." && /[0-9]/.test(sql[i + 1] ?? ""))) {
      while (i < n && /[0-9]/.test(sql[i]!)) i++;
      let float = false;
      if (sql[i] === "." && /[0-9]/.test(sql[i + 1] ?? "")) {
        float = true;
        i++;
        while (i < n && /[0-9]/.test(sql[i]!)) i++;
      } else if (sql[i] === "." && !/[A-Za-z_]/.test(sql[i + 1] ?? "")) {
        float = true; // "1." is a float
        i++;
      }
      if (/[eE]/.test(sql[i] ?? "") && /[0-9+-]/.test(sql[i + 1] ?? "")) {
        float = true;
        i += 2;
        while (i < n && /[0-9]/.test(sql[i]!)) i++;
      }
      out.push({ kind: float ? "float" : "int", value: sql.slice(start, i), span: [start, i] });
      continue;
    }
    if (c === "'" || c === '"') {
      let value = "";
      i++;
      for (;;) {
        if (i >= n) throw new ParseError(c === "'" ? "unterminated string" : "unterminated quoted name", [start, n]);
        if (sql[i] === c) {
          if (sql[i + 1] === c) {
            value += c;
            i += 2;
            continue;
          }
          i++;
          break;
        }
        value += sql[i++];
      }
      out.push({ kind: c === "'" ? "string" : "quoted", value, span: [start, i] });
      continue;
    }
    const op = OPERATORS.find((o) => sql.startsWith(o, i));
    if (!op) throw new ParseError(`unexpected character '${c}'`, [i, i + 1]);
    i += op.length;
    out.push({ kind: "op", value: op === "!=" ? "<>" : op, span: [start, i] });
  }
  out.push({ kind: "eof", value: "", span: [n, n] });
  return out;
}
