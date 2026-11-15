// CAST(x AS t): which conversions exist, and how they work. A conversion that is not
// listed is an analysis error; one that fails on a value (CAST('x' AS int)) is an
// execution error.

import { ExecutionError } from "../errors.ts";
import {
  DAY_US, formatValue, parseDate, parseTimestamp, type DataType, type Value,
} from "../types.ts";
import type { Impl } from "./registry.ts";

const NUMBER = /^[+-]?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?$/i;

function parseNumber(s: string, to: string): number {
  const t = s.trim();
  const n = NUMBER.test(t) ? Number(t) : NaN; // Number("") is 0 and Number("0x1f") is 31
  if (!Number.isFinite(n)) throw new ExecutionError(`cannot cast '${s}' to ${to}`);
  return n;
}

export function checkedInt(n: number, what: () => string): number {
  const i = Math.trunc(n) || 0; // `|| 0`: no -0
  if (!Number.isSafeInteger(i)) throw new ExecutionError(`${what()} is out of range for int (±2^53)`);
  return i;
}

export function stringToTimestamp(s: string, fn = "cast"): number {
  const t = parseTimestamp(s);
  if (t === null) throw new ExecutionError(`${fn}: '${s}' is not a timestamp`);
  return t;
}

export function stringToDate(s: string, fn = "cast"): number {
  const d = parseDate(s);
  if (d === null) throw new ExecutionError(`${fn}: '${s}' is not a date`);
  return d;
}

const BOOL_WORDS: Record<string, boolean> = { true: true, false: false };

/** The conversion from `from` to `to`, or null if there is none. */
export function castImpl(from: DataType, to: DataType): Impl | null {
  if (from === to || from === "null") return (v) => v;
  if (to === "string") return (v) => formatValue(v, from);
  switch (to) {
    case "int":
      if (from === "float") return (v) => checkedInt(v as number, () => `${v}`);
      if (from === "bool") return (v) => (v ? 1 : 0);
      if (from === "string") return (v) => checkedInt(parseNumber(v as string, "int"), () => `'${v}'`);
      return null;
    case "float":
      if (from === "int") return (v) => v;
      if (from === "bool") return (v) => (v ? 1 : 0);
      if (from === "string") return (v) => parseNumber(v as string, "float");
      return null;
    case "bool":
      if (from === "int" || from === "float") return (v) => v !== 0;
      if (from === "string") {
        return (v) => {
          const w = BOOL_WORDS[(v as string).trim().toLowerCase()];
          if (w === undefined) throw new ExecutionError(`cannot cast '${v}' to bool`);
          return w;
        };
      }
      return null;
    case "date":
      if (from === "timestamp") return (v) => Math.floor((v as number) / DAY_US);
      if (from === "string") return (v) => stringToDate(v as string);
      return null;
    case "timestamp":
      if (from === "date") return (v) => (v as number) * DAY_US;
      if (from === "string") return (v) => stringToTimestamp(v as string);
      return null;
  }
  return null;
}

/** Applies a cast to one value (constant folding, literals in the analyzer). */
export function castValue(v: Value, from: DataType, to: DataType): Value {
  if (v === null) return null;
  const impl = castImpl(from, to);
  if (!impl) throw new ExecutionError(`cannot cast ${from} to ${to}`);
  return impl(v);
}
