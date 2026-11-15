// Table metadata as the catalog stores it: tables with versioned snapshots, each
// snapshot a list of immutable data files with per-column statistics.

import type { ColumnType, Value } from "../core/types.ts";
import type { BloomFilter, HyperLogLog } from "../storage/sketch.ts";

export interface ColumnDef {
  readonly name: string;
  readonly type: ColumnType;
}

export type Operation = "create" | "append" | "replace" | "revert";

export interface Snapshot {
  readonly version: number;
  /** Microseconds since the epoch; never goes backwards within a table. */
  readonly createdUs: number;
  readonly operation: Operation;
  /** Rows the operation wrote (a revert: the rows of the restored version). */
  readonly rows: number;
  /** The table's schema as of this version. */
  readonly columns: readonly ColumnDef[];
  readonly detail: string;
}

export interface ColumnStats {
  readonly min: Value;
  readonly max: Value;
  readonly nullCount: number;
  /** Exact number of distinct non-null values in the file. */
  readonly distinct: number | null;
  readonly bloom: BloomFilter | null;
  readonly sketch: HyperLogLog | null;
}

export interface DataFile {
  /** Row id in the catalog's files table (one per snapshot listing). */
  readonly id: number;
  readonly path: string;
  readonly rowCount: number;
  readonly sizeBytes: number;
  readonly position: number;
  readonly stats: ReadonlyMap<string, ColumnStats>;
}

export class TableMeta {
  readonly id: number;
  readonly name: string;
  readonly snapshots: readonly Snapshot[];
  /** The folder of its data files in the file store. */
  readonly location: string;
  constructor(id: number, name: string, snapshots: readonly Snapshot[], location: string) {
    this.id = id;
    this.name = name;
    this.snapshots = snapshots;
    this.location = location;
  }
  get current(): Snapshot {
    return this.snapshots[this.snapshots.length - 1]!;
  }
  get version(): number {
    return this.current.version;
  }
  get columns(): readonly ColumnDef[] {
    return this.current.columns;
  }
  snapshot(version: number): Snapshot | undefined {
    return this.snapshots.find((s) => s.version === version);
  }
  /** The newest snapshot created at or before `micros`. */
  snapshotAsOf(micros: number): Snapshot | undefined {
    let found: Snapshot | undefined;
    for (const s of this.snapshots) if (s.createdUs <= micros) found = s;
    return found;
  }
}

/** What the analyzer needs from a catalog. */
export interface CatalogReader {
  loadTable(name: string): TableMeta;
  files(meta: TableMeta, version: number): DataFile[];
}
