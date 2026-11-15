// The built-in scalar functions and operators. Each is registered by name; the SQL
// parser maps operators to names: `a + b` is "+", `a = b` is "=", `a AND b` is "and",
// `-a` is "neg", `a IS NULL` is "is_null", `a || b` is "concat", `a IN (...)` is "in",
// `a LIKE p` is "like".
//
// Semantics:
// - int arithmetic stays int and fails outside ±2^53; `/` is always float division;
//   `%` takes the dividend's sign (as in SQL); dividing by zero fails.
// - comparisons need the same type after widening (int with float, date with timestamp).
// - and / or / not are three-valued; a filter keeps a row only when it is TRUE.
// - strings are compared by code point; length and substring count code points.

import { ExecutionError } from "../errors.ts";
import {
  DAY_MS, DAY_US, compareValues, type DataType, type Value,
} from "../types.ts";
import { checkedInt, stringToDate, stringToTimestamp } from "./cast.ts";
import {
  TypeMismatch, commonType, overloads, register, type Evaluator, type Impl, type Overload,
} from "./registry.ts";

const infix = (op: string) => (a: readonly string[]) => `(${a[0]} ${op} ${a[1]})`;

// --- arithmetic ------------------------------------------------------------------

function intResult(r: number, a: Value, op: string, b: Value): number {
  if (!Number.isSafeInteger(r)) throw new ExecutionError(`integer overflow in ${a} ${op} ${b} (ints are limited to ±2^53)`);
  return r;
}

function floatResult(r: number, a: Value, op: string, b: Value): number {
  if (!Number.isFinite(r)) throw new ExecutionError(`numeric overflow in ${a} ${op} ${b}`);
  return r;
}

function nonZero(b: Value, op: string): number {
  if (b === 0) throw new ExecutionError(op === "/" ? "division by zero" : "modulo by zero");
  return b as number;
}

const arith: [string, (a: number, b: number) => number, string][] = [
  ["+", (a, b) => a + b, "sum"],
  ["-", (a, b) => a - b, "difference"],
  ["*", (a, b) => a * b, "product"],
  ["%", (a, b) => a % nonZero(b, "%"), "remainder, with the sign of the dividend"],
];
for (const [op, f, doc] of arith) {
  register({
    name: op, category: "arithmetic", operator: true, doc: `${doc} of two numbers`, format: infix(op),
    resolve: overloads([
      { args: ["int", "int"], returns: "int", impl: (a, b) => intResult(f(a as number, b as number) || 0, a, op, b) },
      { args: ["float", "float"], returns: "float", impl: (a, b) => floatResult(f(a as number, b as number), a, op, b) },
    ], `'${op}'`),
  });
}
register({
  name: "/", category: "arithmetic", operator: true, format: infix("/"),
  doc: "division; always a float, even for two ints",
  resolve: overloads([
    { args: ["float", "float"], returns: "float", impl: (a, b) => floatResult((a as number) / nonZero(b, "/"), a, "/", b) },
  ], "'/'"),
});
register({
  name: "neg", category: "arithmetic", operator: true, doc: "negation", format: (a) => `(- ${a[0]})`,
  resolve: overloads([
    { args: ["int"], returns: "int", impl: (a) => -(a as number) || 0 },
    { args: ["float"], returns: "float", impl: (a) => -(a as number) },
  ], "unary '-'"),
});

// --- comparison --------------------------------------------------------------------

const COMPARABLE: DataType[] = ["int", "float", "string", "bool", "date", "timestamp"];
const comparisons: [string, (c: number) => boolean, string][] = [
  ["=", (c) => c === 0, "equal"], ["<>", (c) => c !== 0, "not equal"],
  ["<", (c) => c < 0, "less than"], ["<=", (c) => c <= 0, "at most"],
  [">", (c) => c > 0, "greater than"], [">=", (c) => c >= 0, "at least"],
];
for (const [op, test, doc] of comparisons) {
  const impl: Impl = op === "=" ? (a, b) => a === b : op === "<>" ? (a, b) => a !== b : (a, b) => test(compareValues(a, b));
  register({
    name: op, category: "comparison", operator: true, doc, format: infix(op),
    resolve: overloads(COMPARABLE.map((t) => ({ args: [t, t], returns: "bool", impl })), `'${op}'`),
  });
}

/** x IN (a, b, ...): TRUE on a match; else NULL if x or any candidate is null. */
register({
  name: "in", category: "comparison", operator: true, doc: "membership in a list",
  format: (a) => `(${a[0]} IN (${a.slice(1).join(", ")}))`,
  resolve: (types) => {
    const t = commonType(types);
    if (t === null) throw new TypeMismatch(`IN needs values of one type, got (${types.join(", ")})`);
    return {
      type: "bool", argTypes: types.map(() => t),
      compile: ([x, ...list]) => (row) => {
        const v = x!(row);
        if (v === null) return null;
        let sawNull = false;
        for (const e of list) {
          const c = e(row);
          if (c === v) return true;
          if (c === null) sawNull = true;
        }
        return sawNull ? null : false;
      },
    };
  },
});

const likeCache = new Map<string, RegExp>();
function likeRegex(p: string): RegExp {
  let re = likeCache.get(p);
  if (!re) {
    const body = Array.from(p, (ch) => (ch === "%" ? "[\\s\\S]*" : ch === "_" ? "[\\s\\S]" : ch.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))).join("");
    re = new RegExp(`^${body}$`, "u");
    if (likeCache.size > 256) likeCache.clear();
    likeCache.set(p, re);
  }
  return re;
}
register({
  name: "like", category: "comparison", operator: true, format: infix("LIKE"),
  doc: "pattern match: % is any run of characters, _ one character",
  resolve: overloads([{ args: ["string", "string"], returns: "bool", impl: (s, p) => likeRegex(p as string).test(s as string) }], "LIKE"),
});

// --- logic and nulls -------------------------------------------------------------

/** Three-valued AND / OR; the right side is skipped when the left decides. */
function logic(decisive: boolean): (args: readonly Evaluator[]) => Evaluator {
  return ([l, r]) => (row) => {
    const a = l!(row);
    if (a === decisive) return a;
    const b = r!(row);
    if (b === decisive) return b;
    return a === null || b === null ? null : !decisive;
  };
}
register({
  name: "and", category: "logic", operator: true, doc: "three-valued AND", format: infix("AND"),
  resolve: overloads([{ args: ["bool", "bool"], returns: "bool", compile: logic(false) }], "AND"),
});
register({
  name: "or", category: "logic", operator: true, doc: "three-valued OR", format: infix("OR"),
  resolve: overloads([{ args: ["bool", "bool"], returns: "bool", compile: logic(true) }], "OR"),
});
register({
  name: "not", category: "logic", operator: true, doc: "NOT; NULL stays NULL", format: (a) => `(NOT ${a[0]})`,
  resolve: overloads([{ args: ["bool"], returns: "bool", impl: (a) => !a }], "NOT"),
});
const ANY: DataType[] = ["int", "float", "string", "bool", "date", "timestamp", "null"];
for (const [name, want] of [["is_null", true], ["is_not_null", false]] as const) {
  register({
    name, category: "logic", operator: true, doc: want ? "IS NULL" : "IS NOT NULL",
    format: (a) => `(${a[0]} IS ${want ? "" : "NOT "}NULL)`,
    resolve: overloads(ANY.map((t) => ({ args: [t], returns: "bool", compile: ([x]) => (row) => (x!(row) === null) === want })), name),
  });
}

register({
  name: "coalesce", category: "conditional", doc: "the first argument that is not null",
  signature: "coalesce(x, y, ...) → the arguments' type",
  resolve: (types) => {
    const t = commonType(types);
    if (t === null || !types.length) throw new TypeMismatch(`coalesce needs arguments of one type, got (${types.join(", ")})`);
    return {
      type: t, argTypes: types.map(() => t),
      compile: (args) => (row) => {
        for (const a of args) {
          const v = a(row);
          if (v !== null) return v;
        }
        return null;
      },
    };
  },
});
register({
  name: "nullif", category: "conditional", doc: "NULL if both arguments are equal, else the first",
  signature: "nullif(x, y) → x's type",
  resolve: (types) => {
    const t = commonType(types);
    if (t === null || types.length !== 2) throw new TypeMismatch(`nullif needs two arguments of one type, got (${types.join(", ")})`);
    return {
      type: t, argTypes: [t, t],
      compile: ([x, y]) => (row) => {
        const a = x!(row);
        return a !== null && a === y!(row) ? null : a;
      },
    };
  },
});

// --- strings -----------------------------------------------------------------------

type Simple = [name: string, doc: string, signature: string, overloads: Overload[]];
const S = (impl: Impl, args: DataType[] = ["string"], returns: DataType = "string"): Overload => ({ args, returns, impl });

/** 1-based, by code point; positions outside 1..length are simply empty. */
function substring(s: Value, start: Value, length?: Value): Value {
  const chars = Array.from(s as string);
  const st = start as number;
  if (length !== undefined && (length as number) < 0) throw new ExecutionError(`substring() length must be >= 0, got ${length}`);
  const end = length === undefined ? chars.length + 1 : st + (length as number);
  return chars.slice(Math.max(st, 1) - 1, Math.max(end, 1) - 1).join("");
}

const strings: Simple[] = [
  ["upper", "upper case", "upper(s string) → string", [S((s) => (s as string).toUpperCase())]],
  ["lower", "lower case", "lower(s string) → string", [S((s) => (s as string).toLowerCase())]],
  ["length", "number of characters (code points)", "length(s string) → int", [S((s) => Array.from(s as string).length, ["string"], "int")]],
  ["trim", "without leading and trailing whitespace", "trim(s string) → string", [S((s) => (s as string).trim())]],
  ["ltrim", "without leading whitespace", "ltrim(s string) → string", [S((s) => (s as string).trimStart())]],
  ["rtrim", "without trailing whitespace", "rtrim(s string) → string", [S((s) => (s as string).trimEnd())]],
  ["contains", "whether s contains sub", "contains(s string, sub string) → bool",
    [S((s, x) => (s as string).includes(x as string), ["string", "string"], "bool")]],
  ["starts_with", "whether s starts with prefix", "starts_with(s string, prefix string) → bool",
    [S((s, x) => (s as string).startsWith(x as string), ["string", "string"], "bool")]],
  ["ends_with", "whether s ends with suffix", "ends_with(s string, suffix string) → bool",
    [S((s, x) => (s as string).endsWith(x as string), ["string", "string"], "bool")]],
  ["replace", "every occurrence of old replaced by new", "replace(s string, old string, new string) → string",
    [S((s, o, n) => (o ? (s as string).replaceAll(o as string, n as string) : s), ["string", "string", "string"])]],
  ["substring", "characters from start (1-based), to the end or length of them",
    "substring(s string, start int [, length int]) → string",
    [S((s, a) => substring(s, a), ["string", "int"]), S(substring as Impl, ["string", "int", "int"])]],
  ["substr", "the same as substring", "substr(s string, start int [, length int]) → string",
    [S((s, a) => substring(s, a), ["string", "int"]), S(substring as Impl, ["string", "int", "int"])]],
];
for (const [name, doc, signature, list] of strings) {
  register({ name, category: "string", doc, signature, resolve: overloads(list, name) });
}
register({
  name: "concat", category: "string", doc: "the strings joined; NULL if any is null",
  signature: "concat(s string, ...) → string (also s || t)",
  format: (a) => (a.length === 2 ? `(${a[0]} || ${a[1]})` : `concat(${a.join(", ")})`),
  resolve: overloads([{ args: ["string"], rest: "string", returns: "string", impl: (...xs) => xs.join("") }], "concat"),
});

// --- dates and timestamps -------------------------------------------------------------

const dayOfTs = (us: number) => Math.floor(us / DAY_US);

register({
  name: "to_date", category: "date", doc: "a string (ISO 8601), date or timestamp as a date",
  signature: "to_date(x string | date | timestamp) → date",
  resolve: overloads([
    { args: ["date"], returns: "date", impl: (v) => v },
    { args: ["timestamp"], returns: "date", impl: (v) => dayOfTs(v as number) },
    { args: ["string"], returns: "date", impl: (v) => stringToDate(v as string, "to_date()") },
  ], "to_date"),
});
register({
  name: "to_timestamp", category: "date", doc: "a string (ISO 8601; an offset is converted to UTC), date or timestamp as a timestamp",
  signature: "to_timestamp(x string | date | timestamp) → timestamp",
  resolve: overloads([
    { args: ["timestamp"], returns: "timestamp", impl: (v) => v },
    { args: ["date"], returns: "timestamp", impl: (v) => (v as number) * DAY_US },
    { args: ["string"], returns: "timestamp", impl: (v) => stringToTimestamp(v as string, "to_timestamp()") },
  ], "to_timestamp"),
});
register({
  name: "date_add", category: "date", doc: "a date or timestamp plus a number of days (may be negative)",
  signature: "date_add(d date | timestamp, days int) → d's type",
  resolve: overloads([
    { args: ["date", "int"], returns: "date", impl: (d, n) => checkedInt((d as number) + (n as number), () => "date_add()") },
    { args: ["timestamp", "int"], returns: "timestamp", impl: (t, n) => checkedInt((t as number) + (n as number) * DAY_US, () => "date_add()") },
  ], "date_add"),
});
register({
  name: "date_diff", category: "date", doc: "whole days from the first date to the second",
  signature: "date_diff(a date, b date) → int",
  resolve: overloads([{ args: ["date", "date"], returns: "int", impl: (a, b) => (b as number) - (a as number) }], "date_diff"),
});

const TRUNC_UNITS = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"];
const TIME_STEP: Record<string, number> = { hour: 3_600_000_000, minute: 60_000_000, second: 1_000_000 };

function truncDays(days: number, unit: string): number {
  const day = new Date(days * DAY_MS);
  const y = day.getUTCFullYear(), m = day.getUTCMonth();
  switch (unit) {
    case "year": return Date.UTC(y, 0, 1) / DAY_MS;
    case "quarter": return Date.UTC(y, m - (m % 3), 1) / DAY_MS;
    case "month": return Date.UTC(y, m, 1) / DAY_MS;
    case "week": return days - ((day.getUTCDay() + 6) % 7); // weeks start on Monday
    default: return days;
  }
}
function unitOf(u: Value): string {
  const unit = (u as string).toLowerCase();
  if (!TRUNC_UNITS.includes(unit)) throw new ExecutionError(`date_trunc(): unknown unit '${u}'; expected one of ${TRUNC_UNITS.join(", ")}`);
  return unit;
}
register({
  name: "date_trunc", category: "date",
  doc: "the start of the year, quarter, month, week (Monday), day, hour, minute or second; keeps the type",
  signature: "date_trunc(unit string, d date | timestamp) → d's type",
  resolve: overloads([
    { args: ["string", "date"], returns: "date", impl: (u, d) => (unitOf(u) in TIME_STEP ? d : truncDays(d as number, unitOf(u))) },
    {
      args: ["string", "timestamp"], returns: "timestamp", impl: (u, t) => {
        const unit = unitOf(u), us = t as number, step = TIME_STEP[unit];
        return step ? Math.floor(us / step) * step : truncDays(dayOfTs(us), unit) * DAY_US;
      },
    },
  ], "date_trunc"),
});
const parts: [string, (d: Date) => number][] = [
  ["year", (d) => d.getUTCFullYear()], ["month", (d) => d.getUTCMonth() + 1], ["day", (d) => d.getUTCDate()],
  ["hour", (d) => d.getUTCHours()], ["minute", (d) => d.getUTCMinutes()], ["day_of_week", (d) => ((d.getUTCDay() + 6) % 7) + 1],
];
const PART_DOCS: Record<string, string> = { hour: "the hour (0-23)", minute: "the minute (0-59)", day_of_week: "the day of the week, 1 = Monday to 7 = Sunday" };
for (const [name, get] of parts) {
  register({
    name, category: "date", doc: `${PART_DOCS[name] ?? `the ${name}`} of a date or timestamp`, signature: `${name}(d date | timestamp) → int`,
    resolve: overloads([
      { args: ["date"], returns: "int", impl: (d) => get(new Date((d as number) * DAY_MS)) },
      { args: ["timestamp"], returns: "int", impl: (t) => get(new Date(Math.floor((t as number) / 1000))) },
    ], name),
  });
}

register({
  name: "minutes_between", category: "date", doc: "minutes from the first timestamp to the second, with fractions",
  signature: "minutes_between(a timestamp, b timestamp) → float",
  resolve: overloads([{ args: ["timestamp", "timestamp"], returns: "float", impl: (a, b) => ((b as number) - (a as number)) / 60_000_000 }], "minutes_between"),
});

// --- math --------------------------------------------------------------------------

register({
  name: "abs", category: "math", doc: "absolute value", signature: "abs(x int | float) → x's type",
  resolve: overloads([
    { args: ["int"], returns: "int", impl: (x) => Math.abs(x as number) },
    { args: ["float"], returns: "float", impl: (x) => Math.abs(x as number) },
  ], "abs"),
});
function roundTo(x: number, digits: number): number {
  const f = 10 ** digits;
  const r = Math.round(Math.abs(x) * f) / f; // half away from zero
  return x < 0 ? -r : r;
}
register({
  name: "round", category: "math", doc: "rounded half away from zero, to a number of decimal digits (default 0)",
  signature: "round(x float [, digits int]) → float",
  resolve: overloads([
    { args: ["int"], returns: "int", impl: (x) => x },
    { args: ["float"], returns: "float", impl: (x) => roundTo(x as number, 0) },
    { args: ["float", "int"], returns: "float", impl: (x, d) => roundTo(x as number, d as number) },
  ], "round"),
});
for (const [name, f] of [["floor", Math.floor], ["ceil", Math.ceil]] as const) {
  register({
    name, category: "math", doc: `${name === "floor" ? "largest int not above" : "smallest int not below"} x`,
    signature: `${name}(x float) → int`,
    resolve: overloads([
      { args: ["int"], returns: "int", impl: (x) => x },
      { args: ["float"], returns: "int", impl: (x) => checkedInt(f(x as number), () => `${name}(${x})`) },
    ], name),
  });
}
