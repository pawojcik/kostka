// Writing tables: create, replace and append, each as exactly one snapshot. Tables
// are loaded, not edited: there is no update or delete.
//
// A TableWriter takes rows in any number of `add` calls, writes a data file each time
// it holds the catalog's `file_rows` rows, and commits everything in one snapshot.
//
// - create: a new table, in a new folder.
// - replace: a new snapshot with a new schema and only the new files. History stays,
//   so the old version can be read or reverted to. Creates the table if missing.
// - append: the current schema. Compaction: the live files with fewer than file_rows
//   rows are read first and rewritten with the new rows, so every file but the last
//   is full; those files are removed in the same commit.
//
// Commits are optimistic: if another commit removed a file this append compacts, or
// changed the schema, the append starts again from the new current snapshot.

import { CommitConflictError, SchemaError, TableExistsError, TableNotFoundError } from "../core/errors.ts";
import { validateValue, type Row } from "../core/types.ts";
import { Catalog, CommitConflict, parseColumns } from "../catalog/catalog.ts";
import type { ColumnDef, DataFile, Snapshot, TableMeta } from "../catalog/meta.ts";
import { DEFAULT_FILE_ROWS, randomHex, readDataFile, writeDataFile, type NewFile } from "./data-files.ts";
import { joinPath, type FileStore } from "./file-store.ts";

export type WriteMode = "create" | "replace" | "append";
export const MAX_ATTEMPTS = 50;

export function fileRows(catalog: Catalog): number {
  return catalog.getSetting("file_rows", DEFAULT_FILE_ROWS);
}

export class TableWriter {
  readonly catalog: Catalog;
  readonly store: FileStore;
  readonly table: string;
  readonly detail: string;
  readonly compact: boolean;
  readonly fileRows: number;
  mode: WriteMode;
  columns: readonly ColumnDef[] = [];
  private meta: TableMeta | null = null;
  private location = "";
  private compacted: DataFile[] = [];
  private oldRows = 0;
  private buffer: Row[] = [];
  private written: NewFile[] = [];
  /** Rows added by this writer. */
  rows = 0;
  done = false;

  private constructor(catalog: Catalog, store: FileStore, table: string, mode: WriteMode, detail: string, compact: boolean) {
    this.catalog = catalog;
    this.store = store;
    this.table = table;
    this.mode = mode;
    this.detail = detail;
    this.compact = compact;
    this.fileRows = fileRows(catalog);
  }

  static async open(catalog: Catalog, store: FileStore, table: string, mode: WriteMode, columns: readonly ColumnDef[] | null = null, detail = "", compact = true): Promise<TableWriter> {
    const exists = catalog.tableExists(table);
    if (mode === "create" && exists) throw new TableExistsError(`table '${table}' already exists`);
    if (mode === "append" && !exists) throw new TableNotFoundError(`table '${table}' does not exist`);
    if (mode === "replace" && !exists) mode = "create";
    const w = new TableWriter(catalog, store, table, mode, detail, compact);
    if (mode === "append") {
      if (columns) throw new TypeError("append uses the table's own columns");
      await w.startAppend();
    } else {
      if (!columns) throw new TypeError(`${mode} needs the columns of the new schema`);
      w.columns = parseColumns(columns);
      if (mode === "replace") {
        w.meta = catalog.loadTable(table);
        w.location = w.meta.location;
      } else w.location = `${table}-${randomHex(3)}`;
    }
    return w;
  }

  get names(): string[] {
    return this.columns.map((c) => c.name);
  }

  private async startAppend(): Promise<void> {
    this.meta = this.catalog.loadTable(this.table);
    this.columns = this.meta.columns;
    this.location = this.meta.location;
    this.buffer = [];
    this.written = [];
    const live = this.catalog.files(this.meta);
    this.compacted = this.compact ? live.filter((f) => f.rowCount < this.fileRows) : [];
    for (const f of this.compacted) this.buffer.push(...(await readDataFile(this.store, this.location, f.path, this.names)));
    this.oldRows = this.buffer.length;
  }

  /** Rows as arrays in schema order; every value is validated. */
  async add(rows: Iterable<readonly unknown[]>): Promise<void> {
    const types = this.columns.map((c) => c.type);
    for (const row of rows) {
      if (row.length !== types.length) throw new SchemaError(`${this.table}: ${row.length} values for ${types.length} columns`);
      this.buffer.push(row.map((v, i) => validateValue(v, types[i]!)));
      this.rows++;
    }
    while (this.buffer.length >= this.fileRows) await this.write(this.buffer.splice(0, this.fileRows));
  }

  /** Rows as objects: unknown keys are an error, missing keys are null. */
  async addObjects(rows: Iterable<Record<string, unknown>>): Promise<void> {
    const names = this.names, known = new Set(names);
    const out: unknown[][] = [];
    for (const r of rows) {
      const unknown = Object.keys(r).filter((k) => !known.has(k));
      if (unknown.length) throw new SchemaError(`unknown column(s) ${unknown.map((k) => `'${k}'`).join(", ")} for table '${this.table}'`);
      out.push(names.map((n) => r[n] ?? null));
    }
    await this.add(out);
  }

  private version(): number {
    return this.mode === "create" ? 0 : this.meta!.version + 1;
  }

  private async write(rows: Row[]): Promise<void> {
    try {
      this.written.push(await writeDataFile(this.store, this.location, this.version(), this.columns, rows));
    } catch (e) {
      await this.abort();
      throw e;
    }
  }

  /** Writes the last file and commits one snapshot; null for an append of no rows. */
  async commit(): Promise<Snapshot | null> {
    if (this.done) throw new Error("this writer has already committed or aborted");
    if (this.mode === "append" && !this.rows) {
      await this.abort();
      return null;
    }
    if (this.buffer.length) await this.write(this.buffer.splice(0));
    try {
      for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
        try {
          return this.commitOnce();
        } catch (e) {
          if (!(e instanceof CommitConflict) || this.mode === "create") throw e;
          if (this.mode === "replace") this.meta = this.catalog.loadTable(this.table);
          else await this.restartAppend();
        }
      }
      throw new CommitConflictError(`${this.mode} on '${this.table}' gave up after ${MAX_ATTEMPTS} conflicting commits`);
    } catch (e) {
      await this.abort();
      throw e;
    } finally {
      this.done = true;
    }
  }

  private commitOnce(): Snapshot {
    const add = this.written.map((file) => ({ file, position: null }));
    if (this.mode === "create") return this.catalog.createTable(this.table, this.columns, this.location, this.written, this.rows, this.detail).current;
    if (this.mode === "replace") return this.catalog.commit(this.meta!, "replace", this.rows, add, this.catalog.files(this.meta!), this.columns, this.detail);
    return this.catalog.commit(this.meta!, "append", this.rows, add, this.compacted, null, this.detail);
  }

  /** After a conflict: the new rows are in our files, after the `oldRows` rows from
   * the files we meant to compact. Read them back and start again. */
  private async restartAppend(): Promise<void> {
    const names = this.names;
    const rows: Row[] = [];
    for (const f of this.written) {
      rows.push(...(await readDataFile(this.store, this.location, f.path, names)));
      await this.store.remove(joinPath(this.location, f.path));
    }
    const fresh = rows.slice(this.oldRows);
    await this.startAppend();
    if (this.names.join() !== names.join()) throw new CommitConflictError(`the schema of '${this.table}' changed during the append`);
    this.buffer.push(...fresh);
    while (this.buffer.length) await this.write(this.buffer.splice(0, this.fileRows));
  }

  /** Deletes the files written so far; nothing is committed. */
  async abort(): Promise<void> {
    if (this.mode === "create") await this.store.removeDir(this.location);
    else for (const f of this.written) await this.store.remove(joinPath(this.location, f.path));
    this.written = [];
    this.buffer = [];
    this.done = true;
  }
}
