// The Database facade: tables (create, drop, load, revert, history), loaders (CSV,
// generators, TPC-DS) and queries. Runs on any SqlDb and FileStore (Node or browser).

import { SchemaError, TableNotFoundError } from "../core/errors.ts";
import type { ColumnType, Row, Value } from "../core/types.ts";
import { Catalog, parseColumns } from "../catalog/catalog.ts";
import type { ColumnDef, DataFile, Snapshot, TableMeta } from "../catalog/meta.ts";
import type { SqlDb } from "../catalog/sqlite.ts";
import type { TableReader } from "../physical/operators/scan.ts";
import { FileNotFound, joinPath, type FileStore } from "../storage/file-store.ts";
import { TableWriter, fileRows, type WriteMode } from "../storage/writer.ts";
import { csvRows, inferCsv, type CsvColumn, type CsvOptions } from "../loaders/csv.ts";
import { defaultGenerator, generateRows, type Generator } from "../loaders/generators.ts";
import { generateTpcds, type TpcdsOptions } from "../loaders/tpcds/generate.ts";
import { generateSsb, type SsbOptions } from "../loaders/ssb/generate.ts";
import { loadTaxi, type TaxiOptions } from "../loaders/taxi/load.ts";
import { CompiledQuery, type QueryOptions } from "./query.ts";
import "../core/functions/index.ts";

export const MAX_FILE_ROWS = 1_048_576;
export const MAX_GENERATED_ROWS = 1_000_000;

export interface QueryResult {
  columns: { name: string; type: string }[];
  rows: Row[];
}

export interface GenerateOptions {
  sortBy?: string | null;
  generators?: Record<string, Generator>;
  seed?: number | string;
  mode?: WriteMode;
  /** The schema for create or replace (default: the table's). */
  columns?: readonly ColumnDef[] | readonly (readonly [string, string])[];
}

export class Database {
  readonly catalog: Catalog;
  readonly store: FileStore;
  readonly reader: TableReader;

  constructor(db: SqlDb, store: FileStore) {
    this.catalog = new Catalog(db);
    this.store = store;
    this.reader = {
      readFile: async (scan, file) => {
        try {
          return await store.read(joinPath(scan.meta.location, file.path));
        } catch (e) {
          if (e instanceof FileNotFound) throw new TableNotFoundError(`table '${scan.table}' does not exist (its files are gone)`);
          throw e;
        }
      },
    };
  }

  // --- tables ----------------------------------------------------------------------

  listTables(): string[] {
    return this.catalog.listTables();
  }

  tableExists(name: string): boolean {
    return this.catalog.tableExists(name);
  }

  describe(name: string): TableMeta {
    return this.catalog.loadTable(name);
  }

  files(name: string, version?: number): DataFile[] {
    const meta = this.describe(name);
    return this.catalog.files(meta, version ?? meta.version);
  }

  /** One entry per snapshot, oldest first, with the number of files it has. */
  history(name: string): (Snapshot & { files: number })[] {
    const meta = this.describe(name);
    return meta.snapshots.map((s) => ({ ...s, files: this.catalog.files(meta, s.version).length }));
  }

  get fileRows(): number {
    return fileRows(this.catalog);
  }

  set fileRows(n: number) {
    if (!Number.isInteger(n) || n < 1 || n > MAX_FILE_ROWS) throw new RangeError(`file rows must be an int from 1 to ${MAX_FILE_ROWS.toLocaleString("en-US")}, got ${n}`);
    this.catalog.setSetting("file_rows", n);
  }

  async createTable(name: string, columns: readonly ColumnDef[] | readonly (readonly [string, string])[]): Promise<TableMeta> {
    const w = await this.writer(name, "create", columns);
    await w.commit();
    return this.describe(name);
  }

  async dropTable(name: string): Promise<void> {
    const dir = this.catalog.dropTable(name);
    await this.store.removeDir(dir);
  }

  writer(name: string, mode: WriteMode = "append", columns: readonly ColumnDef[] | readonly (readonly [string, string])[] | null = null, detail = "", compact = true): Promise<TableWriter> {
    return TableWriter.open(this.catalog, this.store, name, mode, columns ? parseColumns(columns as never) : null, detail, compact);
  }

  /** Appends rows (objects, or arrays in schema order) as one snapshot; returns how many. */
  async append(name: string, rows: Iterable<Record<string, unknown> | readonly unknown[]>, compact = true): Promise<number> {
    const w = await this.writer(name, "append", null, "", compact);
    await this.addAll(w, rows);
    await w.commit();
    return w.rows;
  }

  /** Replaces the table's schema and rows in one snapshot (creates it if missing). */
  async replace(name: string, columns: readonly ColumnDef[] | readonly (readonly [string, string])[], rows: Iterable<Record<string, unknown> | readonly unknown[]>, detail = ""): Promise<number> {
    const w = await this.writer(name, "replace", columns, detail);
    await this.addAll(w, rows);
    await w.commit();
    return w.rows;
  }

  private async addAll(w: TableWriter, rows: Iterable<Record<string, unknown> | readonly unknown[]>): Promise<void> {
    const arrays: (readonly unknown[])[] = [], objects: Record<string, unknown>[] = [];
    for (const r of rows) (Array.isArray(r) ? arrays : objects).push(r as never);
    if (arrays.length) await w.add(arrays);
    if (objects.length) await w.addObjects(objects);
  }

  revert(name: string, version: number): Snapshot {
    return this.catalog.revert(name, version);
  }

  // --- loaders ---------------------------------------------------------------------

  inferCsv(text: string, o: CsvOptions = {}) {
    return inferCsv(text, o);
  }

  /** Loads a CSV file into `table` as one snapshot. For append, the included columns
   * are matched to the table's by name and converted to its types; missing ones are
   * null. Returns the number of rows written. */
  async importCsv(text: string, table: string, mode: WriteMode, columns: readonly CsvColumn[] | null = null, o: CsvOptions = {}, detail = "CSV import"): Promise<number> {
    let specs = (columns ?? inferCsv(text, o).columns).filter((c) => c.include);
    if (mode === "append" && this.tableExists(table)) {
      const have = new Map(this.describe(table).columns.map((c) => [c.name, c.type]));
      specs = specs.map((c) => ({ ...c, type: have.get(c.name) ?? c.type }));
    }
    const w = await this.writer(table, mode, mode === "append" ? null : specs.map((c) => [c.name, c.type] as const), detail);
    try {
      const names = w.names;
      const unknown = specs.filter((c) => !names.includes(c.name)).map((c) => c.name);
      if (unknown.length) throw new SchemaError(`table '${table}' has no column(s) ${unknown.join(", ")}`);
      const at = names.map((n) => specs.findIndex((c) => c.name === n));
      let batch: Value[][] = [];
      for (const r of csvRows(text, specs, o)) {
        batch.push(at.map((i) => (i < 0 ? null : r[i]!)));
        if (batch.length >= w.fileRows) {
          await w.add(batch);
          batch = [];
        }
      }
      await w.add(batch);
      await w.commit();
    } catch (e) {
      if (!w.done) await w.abort();
      throw e;
    }
    return w.rows;
  }

  defaultGenerators(name: string): Record<string, Generator> {
    return Object.fromEntries(this.describe(name).columns.map((c) => [c.name, defaultGenerator(c.name, c.type)]));
  }

  /** Generates `n` random rows into `name` as one snapshot (append by default;
   * creates the table when it is missing and `columns` are given). */
  async generate(name: string, n: number, o: GenerateOptions = {}): Promise<number> {
    if (!Number.isInteger(n) || n < 0 || n > MAX_GENERATED_ROWS) throw new RangeError(`rows must be an int from 0 to ${MAX_GENERATED_ROWS.toLocaleString("en-US")}`);
    let mode = o.mode ?? "append";
    let schema: readonly ColumnDef[];
    if (this.tableExists(name)) schema = o.columns ? parseColumns(o.columns as never) : this.describe(name).columns;
    else if (!o.columns) throw new TableNotFoundError(`table '${name}' does not exist; give its columns`);
    else {
      schema = parseColumns(o.columns as never);
      mode = "create";
    }
    const seq = mode === "append" ? this.files(name).reduce((s, f) => s + f.rowCount, 0) : 0;
    const rows = generateRows(schema, n, o.generators ?? {}, o.seed ?? Date.now(), seq, o.sortBy ?? null);
    const w = await this.writer(name, mode, mode === "append" ? null : schema, "generated");
    await w.add(rows);
    await w.commit();
    return n;
  }

  /** Creates or replaces the example tables `users` and `orders`. */
  async loadSample(users = 2000, orders = 20000): Promise<void> {
    await this.generate("users", users, { sortBy: "id", seed: 1, mode: "replace", columns: [["id", "int"], ["name", "string"], ["city", "string"], ["active", "bool"]] });
    await this.generate("orders", orders, {
      sortBy: "id", seed: 2, mode: "replace", columns: [["id", "int"], ["user_id", "int"], ["amount", "float"], ["status", "string"]],
      generators: {
        user_id: { kind: "uniform", min: 1, max: Math.floor(users * 1.1), nulls: 0 },
        status: { kind: "choice", values: ["new", "paid", "shipped", "returned"], nulls: 0 },
      },
    });
  }

  /** Creates the 24 TPC-DS tables at `scale`, replacing tables of the same names. */
  loadTpcds(scale = 0.1, o: TpcdsOptions = {}): Promise<Record<string, number>> {
    return generateTpcds(async (table, columns, detail) => {
      const w = await this.writer(table, "replace", columns, detail);
      return { add: (rows) => w.add(rows), commit: () => w.commit(), abort: () => w.abort() };
    }, scale, o);
  }

  /** Creates the 5 Star Schema Benchmark tables at `scale`, replacing tables of the same names. */
  loadSsb(scale = 0.1, o: SsbOptions = {}): Promise<Record<string, number>> {
    return generateSsb(async (table, columns, detail) => {
      const w = await this.writer(table, "replace", columns, detail);
      return { add: (rows) => w.add(rows), commit: () => w.commit(), abort: () => w.abort() };
    }, scale, o);
  }

  /** Creates the NYC taxi tables trips and zones from the sample's Parquet files
   * (docs/taxi/), replacing tables of the same names. */
  loadTaxi(files: Record<string, Uint8Array>, o: TaxiOptions = {}): Promise<Record<string, number>> {
    return loadTaxi(async (table, columns, detail) => {
      const w = await this.writer(table, "replace", columns, detail);
      return { add: (rows) => w.add(rows), commit: () => w.commit(), abort: () => w.abort() };
    }, files, o);
  }

  // --- queries ---------------------------------------------------------------------

  compile(sql: string, o: QueryOptions = {}): CompiledQuery {
    return new CompiledQuery(sql, this.catalog, this.reader, o);
  }

  async query(sql: string, o: QueryOptions = {}): Promise<QueryResult> {
    const q = this.compile(sql, o);
    const rows = await q.start().execution.run();
    return { columns: q.columns.map((f) => ({ name: f.name, type: f.type as ColumnType })), rows };
  }
}
