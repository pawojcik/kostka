// Column generators for the table editor: each column of a generated table gets one
// (a default chosen from its name and type, or one the user picked).

import { parseDate, parseTimestamp, type ColumnType, type Value } from "../core/types.ts";
import type { ColumnDef } from "../catalog/meta.ts";
import { Random, round } from "./random.ts";

export type Generator =
  | { kind: "sequence"; start?: number; nulls?: number }
  | { kind: "uniform"; min?: number; max?: number; decimals?: number; nulls?: number }
  | { kind: "choice"; values: Value[]; nulls?: number }
  | { kind: "pattern"; prefix?: string; distinct?: number; nulls?: number }
  | { kind: "uuid"; nulls?: number }
  | { kind: "bool"; p_true?: number; nulls?: number }
  | { kind: "date_range"; start: string; end: string; nulls?: number }
  | { kind: "timestamp_range"; start: string; end: string; nulls?: number };

export const GENERATOR_KINDS: Record<ColumnType, Generator["kind"][]> = {
  int: ["sequence", "uniform", "choice"],
  float: ["uniform", "choice"],
  string: ["pattern", "choice", "uuid"],
  bool: ["bool"],
  date: ["date_range"],
  timestamp: ["timestamp_range"],
};

export const CITIES = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan", "Lodz", "Lublin", "Szczecin", "Bydgoszcz", "Katowice", "Torun", "Opole"];
export const NAMES = ["ann", "bob", "cid", "dan", "eve", "fay", "gus", "hal", "ivy", "joe", "kim", "lea", "max", "ned", "ola", "pia", "quinn", "rob", "sam", "tia"];

/** The generator a column gets when the user just clicks "generate". */
export function defaultGenerator(name: string, type: ColumnType): Generator {
  const n = name.toLowerCase();
  switch (type) {
    case "int":
      if (n === "id") return { kind: "sequence", start: 1, nulls: 0 };
      if (n.endsWith("_id")) return { kind: "uniform", min: 1, max: 1000, nulls: 0 };
      return { kind: "uniform", min: 0, max: 1000, nulls: 0.05 };
    case "float":
      return { kind: "uniform", min: 0, max: 100, decimals: 2, nulls: 0.05 };
    case "string": {
      if (n.includes("uuid") || n.includes("guid")) return { kind: "uuid", nulls: 0 };
      const words = n.includes("city") ? CITIES : n.includes("name") ? NAMES : null;
      if (words) return { kind: "choice", values: words, nulls: 0.05 };
      return { kind: "pattern", prefix: `${name}_`, distinct: 20, nulls: 0.05 };
    }
    case "bool":
      return { kind: "bool", p_true: 0.5, nulls: 0.05 };
    case "date":
      return { kind: "date_range", start: "2020-01-01", end: "2024-12-31", nulls: 0.05 };
    case "timestamp":
      return { kind: "timestamp_range", start: "2024-01-01 00:00:00", end: "2024-12-31 23:59:59", nulls: 0.05 };
  }
}

function bounds(g: { kind: string; start: string; end: string }, parse: (s: string) => number | null): [number, number] {
  const lo = parse(String(g.start)), hi = parse(String(g.end));
  if (lo === null || hi === null) throw new RangeError(`generator ${g.kind}: start and end must be ISO dates, like 2024-05-17 or 2024-05-17 13:45:00`);
  if (hi < lo) throw new RangeError(`generator ${g.kind}: end is before start`);
  return [lo, hi];
}

function uuid4(rnd: Random): string {
  const h = rnd.hex(32).split("");
  h[12] = "4";
  h[16] = "89ab"[parseInt(h[16]!, 16) & 3]!;
  const s = h.join("");
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

/** `n` values; `seq` is the sequence number of the first (rows already in the table). */
export function makeValues(g: Generator, type: ColumnType, n: number, rnd: Random, seq = 0): Value[] {
  const nulls = Number(g.nulls ?? 0);
  const out = new Array<Value>(n);
  let one: (i: number) => Value;
  switch (g.kind) {
    case "sequence": {
      const start = Number(g.start ?? 1);
      one = (i) => start + seq + i;
      break;
    }
    case "uniform": {
      const lo = Number(g.min ?? 0), hi = Number(g.max ?? 1000);
      if (hi < lo) throw new RangeError("generator uniform: max is below min");
      one = type === "float" ? () => round(rnd.uniform(lo, hi), Number(g.decimals ?? 2)) : () => rnd.int(Math.ceil(lo), Math.floor(hi));
      break;
    }
    case "choice": {
      if (!g.values?.length) throw new RangeError("generator choice: give at least one value");
      one = () => rnd.choice(g.values);
      break;
    }
    case "pattern": {
      const prefix = g.prefix ?? "v_", k = Math.max(1, Number(g.distinct ?? 20));
      one = () => `${prefix}${rnd.int(0, k - 1)}`;
      break;
    }
    case "uuid":
      one = () => uuid4(rnd);
      break;
    case "bool": {
      const p = Number(g.p_true ?? 0.5);
      one = () => rnd.random() < p;
      break;
    }
    case "date_range": {
      const [lo, hi] = bounds(g, parseDate);
      one = () => rnd.int(lo, hi);
      break;
    }
    case "timestamp_range": {
      const [lo, hi] = bounds(g, parseTimestamp);
      const span = Math.floor((hi - lo) / 1e6);
      one = () => lo + rnd.int(0, span) * 1e6;
      break;
    }
    default:
      throw new RangeError(`unknown generator kind '${(g as { kind: string }).kind}'`);
  }
  for (let i = 0; i < n; i++) out[i] = nulls && rnd.random() < nulls ? null : one(i);
  return out;
}

/** Generates rows for `columns`; `sortBy` sorts them (nulls first) so each data file
 * covers a narrow range of that column. */
export function generateRows(columns: readonly ColumnDef[], n: number, generators: Record<string, Generator>, seed: number | string, seq = 0, sortBy: string | null = null): Value[][] {
  const rnd = new Random(seed);
  const cols = columns.map((c) => makeValues(generators[c.name] ?? defaultGenerator(c.name, c.type), c.type, n, rnd, seq));
  const rows = Array.from({ length: n }, (_, i) => cols.map((c) => c[i]!));
  if (sortBy) {
    const k = columns.findIndex((c) => c.name === sortBy);
    if (k < 0) throw new RangeError(`unknown sort column '${sortBy}'`);
    rows.sort((a, b) => (a[k] === null ? (b[k] === null ? 0 : -1) : b[k] === null ? 1 : a[k]! < b[k]! ? -1 : a[k]! > b[k]! ? 1 : 0));
  }
  return rows;
}
