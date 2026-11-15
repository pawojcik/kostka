// An in-memory CatalogReader for analyzer and optimizer tests: tables with columns and
// optional per-file statistics, no files on disk.
import { TableNotFoundError } from "../../src/core/errors.ts";
import type { ColumnType } from "../../src/core/types.ts";
import { TableMeta, type CatalogReader, type ColumnStats, type DataFile } from "../../src/catalog/meta.ts";

export interface FakeTable {
  columns: [string, ColumnType][];
  files?: { rows: number; stats?: Record<string, Partial<ColumnStats>> }[];
}

export function fakeCatalog(tables: Record<string, FakeTable>): CatalogReader {
  const metas = new Map<string, TableMeta>();
  let id = 0;
  for (const [name, t] of Object.entries(tables)) {
    const columns = t.columns.map(([n, type]) => ({ name: n, type }));
    metas.set(name, new TableMeta(++id, name, [{ version: 0, createdUs: 0, operation: "create", rows: 0, columns, detail: "" }], name));
  }
  return {
    loadTable(name) {
      const m = metas.get(name);
      if (!m) throw new TableNotFoundError(name);
      return m;
    },
    files(meta): DataFile[] {
      return (tables[meta.name]!.files ?? [{ rows: 100 }]).map((f, i) => ({
        id: i, path: `f${i}.parquet`, rowCount: f.rows, sizeBytes: f.rows * 10, position: i,
        stats: new Map(Object.entries(f.stats ?? {}).map(([k, s]) => [k, {
          min: null, max: null, nullCount: 0, distinct: null, bloom: null, sketch: null, ...s,
        } as ColumnStats])),
      }));
    },
  };
}
