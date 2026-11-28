// FileScan: reads a table's pinned snapshot, one partition per data file (a scan
// with no files has one empty partition, so its stage still has a task). Only the
// scan's columns are read, and reading costs storage time for their share of the
// file's bytes. The executor loads the file before the task runs (`load`: decoding
// is async); `execute` emits it. A scan with no columns (`count(*)`) opens no file:
// each file gives its catalog row count of empty rows.

import { Batch } from "../../../../core/batch.ts";
import { COST } from "../../../../core/cost.ts";
import { internal } from "../../../../core/errors.ts";
import type { Row } from "../../../../core/types.ts";
import type { DataFile } from "../../../../catalog/meta.ts";
import type { Scan, ScanInfo } from "../../../../sql/plan.ts";
import { decodeParquet } from "../../../../storage/parquet.ts";
import type { TableReader } from "../../../../storage/table-reader.ts";
import type { TaskContext } from "../context.ts";
import { SparkOp, type BatchIter, type Partitioning } from "../plan.ts";

export class FileScan extends SparkOp {
  readonly scan: ScanInfo;
  private readonly reader: TableReader;

  constructor(node: Scan, reader: TableReader) {
    super(node.schema);
    this.scan = node.info;
    this.reader = reader;
  }

  children(): readonly SparkOp[] {
    return [];
  }

  name(): string {
    return "FileScan";
  }

  args(): string {
    const { table, version, files, snapshotFiles, columns } = this.scan;
    const pruned = files.length === snapshotFiles.length ? "" : `, ${files.length} of ${snapshotFiles.length} files`;
    return `parquet ${table}@v${version} [${columns.join(", ")}]${pruned}`;
  }

  partitioning(): Partitioning {
    return { kind: "unknown", n: Math.max(1, this.scan.files.length) };
  }

  /** The file of partition `p`, or null for a scan with no files. */
  file(p: number): DataFile | null {
    return this.scan.files[p] ?? null;
  }

  /** A Parquet reader fetches only the scan's column chunks: the file's size, shared
   * out by column count. */
  readBytes(f: DataFile): number {
    const total = Math.max(f.stats.size, this.scan.columns.length, 1);
    return Math.floor((f.sizeBytes * this.scan.columns.length) / total);
  }

  /** Loads and decodes partition `p`'s file (null: nothing to load). */
  async load(p: number): Promise<{ rows: Row[]; decodeNs: number } | null> {
    const f = this.file(p);
    if (!f || !this.scan.columns.length) return null;
    const bytes = await this.reader.readFile(this.scan.meta, f);
    const t0 = performance.now();
    const rows = await decodeParquet(bytes, this.scan.columns);
    return { rows, decodeNs: Math.round((performance.now() - t0) * 1e6) };
  }

  execute(ctx: TaskContext, partition: number): BatchIter {
    const f = this.file(partition);
    let rows: Row[] = [];
    if (f && this.scan.columns.length) {
      const data = ctx.loaded(this);
      if (!data) throw internal(`partition ${partition} of ${this.label()} was not loaded`);
      const bytes = this.readBytes(f);
      ctx.readStorage(bytes, this);
      ctx.cpuTaken(data.decodeNs, this);
      ctx.cpu(data.rows.length * this.scan.columns.length * COST.scanValue, this);
      rows = data.rows;
      ctx.note(this, `read ${f.path}: ${rows.length.toLocaleString("en")} rows, ${this.scan.columns.length} column(s), ${bytes.toLocaleString("en")} bytes`);
    } else if (f) {
      rows = Array.from({ length: f.rowCount }, () => []);
      ctx.note(this, `${f.path}: ${f.rowCount.toLocaleString("en")} rows from the catalog, not opened`);
    }
    let given = false;
    return {
      // A file is one row group: one batch.
      next: () => {
        if (given || !rows.length) return null;
        given = true;
        ctx.output(this, rows.length);
        return new Batch(this.output, rows);
      },
    };
  }
}
