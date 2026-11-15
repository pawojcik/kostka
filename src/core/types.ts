// Column types and values.
//
// A value in a row is a plain JS primitive: null, boolean, number or string. What a
// number means comes from the column's type, which the analyzer knows for every
// expression:
//
//   int        an integer in the safe range ±(2^53 - 1); arithmetic that leaves it fails
//   float      a finite double
//   string     a JS string (compared by code point)
//   bool       true / false
//   date       whole days since 1970-01-01
//   timestamp  whole microseconds since 1970-01-01 00:00 UTC (naive, no zone)
//
// "null" is the type of an untyped NULL literal; it fits any other type.

import { SchemaError } from "./errors.ts";

export type DataType = "int" | "float" | "string" | "bool" | "date" | "timestamp" | "null";
export type ColumnType = Exclude<DataType, "null">;
export type Value = null | boolean | number | string;
export type Row = Value[];

export const COLUMN_TYPES: readonly ColumnType[] = ["int", "float", "string", "bool", "date", "timestamp"];

export const INT_MAX = Number.MAX_SAFE_INTEGER;
export const INT_MIN = Number.MIN_SAFE_INTEGER;
export const DAY_MS = 86_400_000;
export const DAY_US = 86_400_000_000;

export function parseType(name: string): ColumnType {
  const t = name.toLowerCase();
  const alias: Record<string, ColumnType> = {
    integer: "int", bigint: "int", double: "float", real: "float", text: "string",
    varchar: "string", boolean: "bool",
  };
  const found = (COLUMN_TYPES as readonly string[]).includes(t) ? (t as ColumnType) : alias[t];
  if (!found) throw new SchemaError(`unknown type '${name}'; expected one of ${COLUMN_TYPES.join(", ")}`);
  return found;
}

export const isNumeric = (t: DataType): boolean => t === "int" || t === "float";
export const isTemporal = (t: DataType): boolean => t === "date" || t === "timestamp";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
export const isValidIdentifier = (name: unknown): boolean => typeof name === "string" && IDENTIFIER.test(name);

// --- dates and timestamps -------------------------------------------------------

const ISO = /^\s*(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?\s*(Z|[+-]\d{2}(?::?\d{2})?)?)?\s*$/i;

/** ISO 8601 text as micros since the epoch ("2024-05-17", "2024-05-17 13:45:00.25",
 * "2024-05-17T13:45Z", an offset is converted to UTC), or null if it isn't one. */
export function parseTimestamp(text: string): number | null {
  const m = ISO.exec(text);
  if (!m) return null;
  const [, y, mo, d, h = "0", mi = "0", s = "0", frac = "", zone] = m;
  const ms = Date.UTC(+y!, +mo! - 1, +d!, +h, +mi, +s);
  const check = new Date(ms);
  if (check.getUTCFullYear() !== +y! || check.getUTCMonth() !== +mo! - 1 || check.getUTCDate() !== +d!
    || +h > 23 || +mi > 59 || +s > 59) {
    return null;
  }
  let micros = ms * 1000 + Number(frac.slice(0, 6).padEnd(6, "0"));
  if (zone && zone.toUpperCase() !== "Z") {
    const sign = zone[0] === "-" ? -1 : 1;
    const digits = zone.slice(1).replace(":", "");
    micros -= sign * (Number(digits.slice(0, 2)) * 60 + Number(digits.slice(2) || "0")) * 60_000_000;
  }
  return Number.isSafeInteger(micros) ? micros : null;
}

/** A date ("2024-05-17", or a timestamp: its UTC day) as days, or null. */
export function parseDate(text: string): number | null {
  const t = parseTimestamp(text);
  return t === null ? null : Math.floor(t / DAY_US);
}

export function daysOf(y: number, m: number, d: number): number {
  return Date.UTC(y, m - 1, d) / DAY_MS;
}

export function formatDate(days: number): string {
  const d = new Date(days * DAY_MS);
  const y = d.getUTCFullYear();
  const year = y >= 0 && y <= 9999 ? String(y).padStart(4, "0") : String(y);
  return `${year}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

/** "YYYY-MM-DD HH:MM:SS", with ".ffffff" when there are microseconds. */
export function formatTimestamp(micros: number): string {
  const days = Math.floor(micros / DAY_US);
  let rest = micros - days * DAY_US;
  const us = rest % 1_000_000;
  rest = (rest - us) / 1_000_000;
  const hh = Math.floor(rest / 3600), mm = Math.floor((rest % 3600) / 60), ss = rest % 60;
  const time = [hh, mm, ss].map((x) => String(x).padStart(2, "0")).join(":");
  return `${formatDate(days)} ${time}${us ? "." + String(us).padStart(6, "0") : ""}`;
}

/** A float as text: always with a fraction or exponent, so 2.0 reads "2.0", not "2". */
export function formatFloat(x: number): string {
  const s = String(x);
  return /[.eEn]/.test(s) ? s : `${s}.0`; // "n": NaN and Infinity never reach here
}

/** A value of type `t` as text (casts to string, CSV export, the workbench). */
export function formatValue(v: Value, t: DataType): string {
  if (v === null) return "NULL";
  switch (t) {
    case "float": return formatFloat(v as number);
    case "date": return formatDate(v as number);
    case "timestamp": return formatTimestamp(v as number);
    case "bool": return v ? "true" : "false";
    default: return String(v);
  }
}

/** A value of type `t` as a SQL literal, for EXPLAIN and error messages. */
export function sqlLiteral(v: Value, t: DataType): string {
  if (v === null) return "NULL";
  switch (t) {
    case "string": return `'${(v as string).replaceAll("'", "''")}'`;
    case "date": return `DATE '${formatDate(v as number)}'`;
    case "timestamp": return `TIMESTAMP '${formatTimestamp(v as number)}'`;
    case "bool": return v ? "TRUE" : "FALSE";
    default: return formatValue(v, t);
  }
}

// --- comparing and hashing ---------------------------------------------------

/** Compare strings by code point (JS's `<` compares UTF-16 code units). */
export function compareStrings(a: string, b: string): number {
  if (a === b) return 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.charCodeAt(i), y = b.charCodeAt(i);
    if (x !== y) {
      // Only surrogates (U+D800..DFFF) sort differently from code points.
      const sx = x >= 0xd800 && x <= 0xdfff, sy = y >= 0xd800 && y <= 0xdfff;
      if (sx !== sy) return sx ? 1 : -1;
      return x < y ? -1 : 1;
    }
  }
  return a.length < b.length ? -1 : 1;
}

/** Order of two non-null values of the same type. */
export function compareValues(a: Value, b: Value): number {
  if (typeof a === "string") return compareStrings(a, b as string);
  return a! < b! ? -1 : a! > b! ? 1 : 0; // numbers; booleans as false < true
}

/**
 * A key for a tuple of values, usable in a Map: equal exactly when the values are.
 * One value is its own key (a Map tells 1 from "1" and from true). Several are joined
 * with a type tag and a length, so ("a|", "b") and ("a", "|b") differ.
 */
export function tupleKey(values: readonly Value[]): Value {
  if (values.length === 1) return values[0]!;
  let out = "";
  for (const v of values) {
    const s = v === null ? "_" : typeof v === "string" ? `s${v}` : typeof v === "number" ? `n${v}` : v ? "T" : "F";
    out += `${s.length}:${s}`;
  }
  return out;
}

// --- validation of values to store ---------------------------------------------

/** `value` normalised for a column of type `t`, or a SchemaError. Accepts the JS
 * values a caller would naturally hold: numbers, strings, booleans, Date objects. */
export function validateValue(value: unknown, t: ColumnType): Value {
  if (value === null || value === undefined) return null;
  const bad = () => new SchemaError(`value ${describeUnknown(value)} is not a valid ${t}`);
  switch (t) {
    case "int":
      if (typeof value === "bigint") {
        if (value > BigInt(INT_MAX) || value < BigInt(INT_MIN)) {
          throw new SchemaError(`value ${value} is out of range for int (±2^53)`);
        }
        return Number(value);
      }
      if (typeof value !== "number" || !Number.isInteger(value)) throw bad();
      if (!Number.isSafeInteger(value)) throw new SchemaError(`value ${value} is out of range for int (±2^53)`);
      return value;
    case "float":
      if (typeof value !== "number" || !Number.isFinite(value)) throw bad();
      return value;
    case "string":
      if (typeof value !== "string") throw bad();
      return value;
    case "bool":
      if (typeof value !== "boolean") throw bad();
      return value;
    case "date":
      if (value instanceof Date) return Math.floor(value.getTime() / DAY_MS);
      if (typeof value === "number" && Number.isSafeInteger(value)) return value;
      throw bad();
    case "timestamp":
      if (value instanceof Date) return value.getTime() * 1000;
      if (typeof value === "number" && Number.isSafeInteger(value)) return value;
      throw bad();
  }
}

function describeUnknown(v: unknown): string {
  if (typeof v === "string") return `'${v}'`;
  if (typeof v === "bigint") return `${v}n`;
  if (v instanceof Date) return `Date(${v.toISOString()})`;
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}
