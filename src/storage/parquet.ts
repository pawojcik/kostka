// Data files are Parquet files with one row group, written and read with hyparquet:
// int -> INT64, float -> DOUBLE, string -> UTF8, bool -> BOOLEAN, date -> INT32 DATE,
// timestamp -> INT64 TIMESTAMP(MICROS). Readers ask for the columns they need only.
// An INT64 that doesn't fit a JS number exactly (|n| > 2^53) is an error.

import { parquetMetadata, parquetReadObjects } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { ExecutionError } from "../core/errors.ts";
import type { ColumnType, Row, Value } from "../core/types.ts";
import type { ColumnDef } from "../catalog/meta.ts";

function element(name: string, type: ColumnType): Record<string, unknown> {
  const base = { name, repetition_type: "OPTIONAL" };
  switch (type) {
    case "int": return { ...base, type: "INT64" };
    case "float": return { ...base, type: "DOUBLE" };
    case "string": return { ...base, type: "BYTE_ARRAY", converted_type: "UTF8", logical_type: { type: "STRING" } };
    case "bool": return { ...base, type: "BOOLEAN" };
    case "date": return { ...base, type: "INT32", converted_type: "DATE", logical_type: { type: "DATE" } };
    case "timestamp": return { ...base, type: "INT64", logical_type: { type: "TIMESTAMP", isAdjustedToUTC: false, unit: "MICROS" } };
  }
}

function physical(v: Value, type: ColumnType): unknown {
  if (v === null) return null;
  return type === "int" || type === "timestamp" ? BigInt(v as number) : v;
}

export function encodeParquet(columns: readonly ColumnDef[], rows: readonly Row[]): Uint8Array {
  const schema = [{ name: "schema", num_children: columns.length }, ...columns.map((c) => element(c.name, c.type))];
  const columnData = columns.map((c, i) => ({ name: c.name, data: rows.map((r) => physical(r[i]!, c.type)) }));
  return new Uint8Array(parquetWriteBuffer({ columnData, schema, rowGroupSize: Math.max(rows.length, 1) } as never));
}

function int(v: bigint): number {
  const n = Number(v);
  if (!Number.isSafeInteger(n)) throw new ExecutionError(`int ${v} does not fit kostka's ints (at most 2^53 - 1 in size)`);
  return n;
}

const parsers = {
  dateFromDays: (days: number) => days,
  timestampFromMicroseconds: (us: bigint) => int(us),
  timestampFromMilliseconds: (ms: bigint) => int(ms) * 1000,
  timestampFromNanoseconds: (ns: bigint) => int(ns / 1000n),
};

/** Rows of the named columns only. */
export async function decodeParquet(bytes: Uint8Array, names: readonly string[]): Promise<Row[]> {
  const file = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  if (!names.length) return Array.from({ length: Number(parquetMetadata(file).num_rows) }, () => []);
  const objects = (await parquetReadObjects({ file, columns: [...names], parsers, utf8: true } as never)) as Record<string, unknown>[];
  const out = new Array<Row>(objects.length);
  for (let i = 0; i < objects.length; i++) {
    const o = objects[i]!;
    const r = new Array<Value>(names.length);
    for (let j = 0; j < names.length; j++) {
      const v = o[names[j]!];
      r[j] = typeof v === "bigint" ? int(v) : ((v as Value) ?? null);
    }
    out[i] = r;
  }
  return out;
}

export function parquetRowCount(bytes: Uint8Array): number {
  const file = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  return Number(parquetMetadata(file).num_rows);
}
