// The catalog: table metadata in SQLite.
//
//   tables        id, name, dir (its data files' folder in the file store)
//   snapshots     one per version of a table, with that version's schema
//   files         data files: added_in a version, removed_in a later one (or never)
//   file_columns  per-file column statistics
//   settings      key -> JSON value
//
// A commit is one SQLite transaction that appends a snapshot and records which files
// it adds and removes (optimistic concurrency: it fails with CommitConflict if a file
// it removes is no longer live). Nothing is ever purged: the files of a snapshot are
// the ones with added_in <= version and no removed_in <= version. A revert adds a
// snapshot that lists the old version's files again.
//
// Data files are not the catalog's business: callers write them first and delete a
// dropped table's folder themselves.

import { CatalogError, SchemaError, TableExistsError, TableNotFoundError } from "../core/errors.ts";
import { isValidIdentifier, parseType, type ColumnType, type Value } from "../core/types.ts";
import { BloomFilter, HyperLogLog } from "../storage/sketch.ts";
import { TableMeta, type CatalogReader, type ColumnDef, type ColumnStats, type DataFile, type Operation, type Snapshot } from "./meta.ts";
import { one, type SqlDb, type SqlValue } from "./sqlite.ts";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS tables (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  dir TEXT NOT NULL,
  created_us INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  created_us INTEGER NOT NULL,
  operation TEXT NOT NULL,
  rows INTEGER NOT NULL,
  schema TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (table_id, version)
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  position INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  size_bytes INTEGER NOT NULL,
  added_in INTEGER NOT NULL,
  removed_in INTEGER
);
CREATE INDEX IF NOT EXISTS files_by_table ON files (table_id, added_in);
CREATE TABLE IF NOT EXISTS file_columns (
  file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  column_name TEXT NOT NULL,
  min_value, max_value,
  null_count INTEGER NOT NULL,
  distinct_count INTEGER,
  bloom BLOB,
  hll BLOB,
  PRIMARY KEY (file_id, column_name)
);`;

/** A file a commit removes is no longer live, or the schema changed under it. */
export class CommitConflict extends Error {
  constructor(what: string) {
    super(`commit conflict: ${what}`);
    this.name = "CommitConflict";
  }
}

export function parseColumns(columns: readonly (readonly [string, string] | ColumnDef)[]): ColumnDef[] {
  if (!columns.length) throw new SchemaError("a table needs at least one column");
  const seen = new Set<string>();
  return columns.map((c) => {
    const [name, type] = Array.isArray(c) ? c : [(c as ColumnDef).name, (c as ColumnDef).type];
    if (!isValidIdentifier(name)) throw new SchemaError(`invalid column name '${name}'`);
    if (seen.has(name)) throw new SchemaError(`duplicate column '${name}'`);
    seen.add(name);
    return { name, type: parseType(type) };
  });
}

const schemaJson = (cols: readonly ColumnDef[]) => JSON.stringify(cols.map((c) => [c.name, c.type]));
const schemaFrom = (text: string): ColumnDef[] => (JSON.parse(text) as [string, ColumnType][]).map(([name, type]) => ({ name, type }));

const toSql = (t: ColumnType, v: Value): SqlValue => (v === null ? null : t === "bool" ? (v ? 1 : 0) : (v as number | string));
const fromSql = (t: ColumnType, v: SqlValue): Value => (v === null ? null : t === "bool" ? v === 1 : (v as number | string));

export function nowMicros(): number {
  return Date.now() * 1000;
}

export interface NewFileEntry {
  readonly file: Omit<DataFile, "id" | "position">;
  /** The position it takes; null appends it after the table's other files. */
  readonly position: number | null;
}

export class Catalog implements CatalogReader {
  readonly db: SqlDb;
  constructor(db: SqlDb) {
    this.db = db;
    db.exec(SCHEMA);
  }

  private tx<T>(fn: () => T, write = true): T {
    this.db.exec(write ? "BEGIN IMMEDIATE" : "BEGIN");
    try {
      const r = fn();
      this.db.exec("COMMIT");
      return r;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }

  // --- settings ------------------------------------------------------------------

  getSetting<T>(key: string, fallback: T): T {
    const r = one(this.db, "SELECT value FROM settings WHERE key = ?", [key]);
    return r ? (JSON.parse(r.value as string) as T) : fallback;
  }

  setSetting(key: string, value: unknown): void {
    this.db.run("INSERT INTO settings VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", [key, JSON.stringify(value)]);
  }

  // --- tables --------------------------------------------------------------------

  tableExists(name: string): boolean {
    return isValidIdentifier(name) && !!one(this.db, "SELECT 1 AS x FROM tables WHERE name = ?", [name]);
  }

  listTables(): string[] {
    return this.db.all("SELECT name FROM tables ORDER BY name").map((r) => r.name as string);
  }

  /** Creates `name` with snapshot 0 holding `files` (already written into `dir`). */
  createTable(name: string, columns: readonly ColumnDef[], dir: string, files: NewFileEntry["file"][] = [], rows = 0, detail = ""): TableMeta {
    if (!isValidIdentifier(name)) throw new SchemaError(`invalid table name '${name}'`);
    const defs = parseColumns(columns);
    const now = nowMicros();
    this.tx(() => {
      if (one(this.db, "SELECT 1 AS x FROM tables WHERE name = ?", [name])) throw new TableExistsError(`table '${name}' already exists`);
      const id = this.db.run("INSERT INTO tables (name, dir, created_us) VALUES (?, ?, ?)", [name, dir, now]).lastId;
      this.db.run("INSERT INTO snapshots VALUES (?, 0, ?, 'create', ?, ?, ?)", [id, now, rows, schemaJson(defs), detail]);
      this.addFiles(id, 0, defs, files.map((file) => ({ file, position: null })));
    });
    return this.loadTable(name);
  }

  /** Removes the table's metadata; returns its data folder, for the caller to delete. */
  dropTable(name: string): string {
    return this.tx(() => {
      const r = one(this.db, "SELECT dir FROM tables WHERE name = ?", [name]);
      if (!r) throw new TableNotFoundError(`table '${name}' does not exist`);
      this.db.run("DELETE FROM tables WHERE name = ?", [name]);
      return r.dir as string;
    });
  }

  loadTable(name: string): TableMeta {
    if (!isValidIdentifier(name)) throw new TableNotFoundError(`table '${name}' does not exist`);
    return this.tx(() => {
      const r = one(this.db, "SELECT id, dir FROM tables WHERE name = ?", [name]);
      if (!r) throw new TableNotFoundError(`table '${name}' does not exist`);
      const snaps: Snapshot[] = this.db.all(
        "SELECT version, created_us, operation, rows, schema, detail FROM snapshots WHERE table_id = ? ORDER BY version", [r.id!],
      ).map((s) => ({
        version: s.version as number, createdUs: s.created_us as number, operation: s.operation as Operation,
        rows: s.rows as number, columns: schemaFrom(s.schema as string), detail: s.detail as string,
      }));
      return new TableMeta(r.id as number, name, snaps, r.dir as string);
    }, false);
  }

  // --- files ---------------------------------------------------------------------

  /** The data files of a snapshot, in order, with their column statistics. */
  files(meta: TableMeta, version: number = meta.version): DataFile[] {
    const snap = meta.snapshot(version);
    if (!snap) throw new CatalogError(`table '${meta.name}' has no snapshot version ${version}`);
    const types = new Map(snap.columns.map((c) => [c.name, c.type]));
    return this.tx(() => {
      const live = "table_id = ? AND added_in <= ? AND (removed_in IS NULL OR removed_in > ?)";
      const rows = this.db.all(`SELECT id, path, row_count, size_bytes, position FROM files WHERE ${live} ORDER BY position`, [meta.id, version, version]);
      const stats = new Map<number, Map<string, ColumnStats>>();
      for (const s of this.db.all(
        "SELECT fc.* FROM file_columns fc JOIN files f ON f.id = fc.file_id WHERE f.table_id = ? AND f.added_in <= ? AND (f.removed_in IS NULL OR f.removed_in > ?)",
        [meta.id, version, version],
      )) {
        const t = types.get(s.column_name as string);
        if (!t) continue;
        const m = stats.get(s.file_id as number) ?? new Map<string, ColumnStats>();
        m.set(s.column_name as string, {
          min: fromSql(t, s.min_value!), max: fromSql(t, s.max_value!), nullCount: s.null_count as number,
          distinct: (s.distinct_count as number | null) ?? null,
          bloom: s.bloom ? BloomFilter.fromBytes(s.bloom as Uint8Array) : null,
          sketch: s.hll ? HyperLogLog.fromBytes(s.hll as Uint8Array) : null,
        });
        stats.set(s.file_id as number, m);
      }
      return rows.map((r) => ({
        id: r.id as number, path: r.path as string, rowCount: r.row_count as number, sizeBytes: r.size_bytes as number,
        position: r.position as number, stats: stats.get(r.id as number) ?? new Map(),
      }));
    }, false);
  }

  /** Every data file path any snapshot of the table refers to. */
  allPaths(name: string): Set<string> {
    const meta = this.loadTable(name);
    return new Set(this.db.all("SELECT path FROM files WHERE table_id = ?", [meta.id]).map((r) => r.path as string));
  }

  // --- commits -------------------------------------------------------------------

  /** Appends snapshot current + 1: `remove` gets removed_in, `add` added_in. `columns`
   * is the new schema (null: unchanged, and it must not have changed since `meta`). */
  commit(meta: TableMeta, operation: Operation, rows: number, add: readonly NewFileEntry[], remove: readonly DataFile[], columns: readonly ColumnDef[] | null = null, detail = ""): Snapshot {
    return this.tx(() => {
      const r = one(this.db, "SELECT id FROM tables WHERE name = ?", [meta.name]);
      if (!r || r.id !== meta.id) throw new TableNotFoundError(`table '${meta.name}' does not exist`);
      const last = one(this.db, "SELECT version, created_us, schema FROM snapshots WHERE table_id = ? ORDER BY version DESC LIMIT 1", [meta.id])!;
      let cols: ColumnDef[];
      if (columns === null) {
        if (last.schema !== schemaJson(meta.columns)) throw new CommitConflict("the schema changed");
        cols = [...meta.columns];
      } else cols = parseColumns(columns);
      for (const f of remove) {
        const live = one(this.db, "SELECT removed_in IS NULL AS live FROM files WHERE id = ?", [f.id]);
        if (!live || !live.live) throw new CommitConflict(`${f.path} was replaced`);
      }
      const version = (last.version as number) + 1;
      const createdUs = Math.max(nowMicros(), (last.created_us as number) + 1);
      this.db.run("INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?, ?)", [meta.id, version, createdUs, operation, rows, schemaJson(cols), detail]);
      for (const f of remove) this.db.run("UPDATE files SET removed_in = ? WHERE id = ?", [version, f.id]);
      this.addFiles(meta.id, version, cols, add);
      return { version, createdUs, operation, rows, columns: cols, detail };
    });
  }

  private addFiles(tableId: number, version: number, columns: readonly ColumnDef[], add: readonly NewFileEntry[]): void {
    let next = one(this.db, "SELECT COALESCE(MAX(position), -1) + 1 AS n FROM files WHERE table_id = ?", [tableId])!.n as number;
    const types = new Map(columns.map((c) => [c.name, c.type]));
    for (const { file, position } of add) {
      const pos = position ?? next++;
      const id = this.db.run("INSERT INTO files (table_id, path, position, row_count, size_bytes, added_in) VALUES (?, ?, ?, ?, ?, ?)",
        [tableId, file.path, pos, file.rowCount, file.sizeBytes, version]).lastId;
      for (const [col, s] of file.stats) {
        const t = types.get(col)!;
        this.db.run("INSERT INTO file_columns VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
          id, col, toSql(t, s.min), toSql(t, s.max), s.nullCount, s.distinct, s.bloom?.toBytes() ?? null, s.sketch?.toBytes() ?? null,
        ]);
      }
    }
  }

  /** A new snapshot with the files and schema of `version`. Later snapshots stay, so
   * a revert can itself be reverted. */
  revert(name: string, version: number): Snapshot {
    return this.tx(() => {
      const r = one(this.db, "SELECT id FROM tables WHERE name = ?", [name]);
      if (!r) throw new TableNotFoundError(`table '${name}' does not exist`);
      const id = r.id as number;
      const target = one(this.db, "SELECT schema FROM snapshots WHERE table_id = ? AND version = ?", [id, version]);
      if (!target) throw new CatalogError(`table '${name}' has no snapshot version ${version}`);
      const last = one(this.db, "SELECT version, created_us FROM snapshots WHERE table_id = ? ORDER BY version DESC LIMIT 1", [id])!;
      const v = (last.version as number) + 1;
      const old = this.db.all("SELECT id, path, position, row_count, size_bytes FROM files WHERE table_id = ? AND added_in <= ? AND (removed_in IS NULL OR removed_in > ?)", [id, version, version]);
      const rows = old.reduce((s, f) => s + (f.row_count as number), 0);
      const createdUs = Math.max(nowMicros(), (last.created_us as number) + 1);
      const detail = `to v${version}`;
      this.db.run("INSERT INTO snapshots VALUES (?, ?, ?, 'revert', ?, ?, ?)", [id, v, createdUs, rows, target.schema!, detail]);
      this.db.run("UPDATE files SET removed_in = ? WHERE table_id = ? AND removed_in IS NULL AND added_in < ?", [v, id, v]);
      for (const f of old) {
        const fid = this.db.run("INSERT INTO files (table_id, path, position, row_count, size_bytes, added_in) VALUES (?, ?, ?, ?, ?, ?)",
          [id, f.path!, f.position!, f.row_count!, f.size_bytes!, v]).lastId;
        this.db.run("INSERT INTO file_columns SELECT ?, column_name, min_value, max_value, null_count, distinct_count, bloom, hll FROM file_columns WHERE file_id = ?", [fid, f.id!]);
      }
      return { version: v, createdUs, operation: "revert" as const, rows, columns: schemaFrom(target.schema as string), detail };
    });
  }
}
