// Writing and reading a table's data files through a FileStore.

import type { Row } from "../core/types.ts";
import type { ColumnDef, ColumnStats, DataFile } from "../catalog/meta.ts";
import { joinPath, type FileStore } from "./file-store.ts";
import { decodeParquet, encodeParquet } from "./parquet.ts";
import { columnStats } from "./stats.ts";

export const DEFAULT_FILE_ROWS = 8192;

/** A written file that is not committed yet. */
export type NewFile = Omit<DataFile, "id" | "position">;

export function randomHex(bytes = 4): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (b) => b.toString(16).padStart(2, "0")).join("");
}

export async function writeDataFile(store: FileStore, location: string, version: number, columns: readonly ColumnDef[], rows: readonly Row[]): Promise<NewFile> {
  const path = `data-${String(version).padStart(6, "0")}-${randomHex()}.parquet`;
  const bytes = encodeParquet(columns, rows);
  await store.write(joinPath(location, path), bytes);
  const stats = new Map<string, ColumnStats>(columns.map((c, i) => [c.name, columnStats(c.type, rows.map((r) => r[i]!))]));
  return { path, rowCount: rows.length, sizeBytes: bytes.length, stats };
}

export async function readDataFile(store: FileStore, location: string, path: string, names: readonly string[]): Promise<Row[]> {
  return decodeParquet(await store.read(joinPath(location, path)), names);
}
