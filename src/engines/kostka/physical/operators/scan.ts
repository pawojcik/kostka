// Reads a table's pinned snapshot: one split per data file, only the scan's columns.
// A file is one row group, so it is emitted as one batch. Reading costs storage time
// for the scan's share of the file's bytes. A scan with no columns (`count(*)`) opens
// no file: each file contributes its catalog row count of empty rows.

import { internal } from "../../../../core/errors.ts";
import type { Row } from "../../../../core/types.ts";
import { decodeParquet } from "../../../../storage/parquet.ts";
import type { TableReader } from "../../../../storage/table-reader.ts";
import type { DataFile } from "../../../../catalog/meta.ts";
import type { Scan, ScanInfo } from "../../../../sql/plan.ts";
import { Batch } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST } from "../../../../core/cost.ts";
import type { FileSplit, Split } from "../buffer.ts";
import { Source, type SourceInput, type SourceInstance } from "../operator.ts";
import type { RuntimeFilter } from "./runtime-filter.ts";


export class ScanSource extends Source {
  readonly scan: ScanInfo;
  private readonly reader: TableReader;
  /** Runtime filters on its columns: files whose stats miss the keys get no split. */
  readonly runtimeFilters: RuntimeFilter[] = [];

  constructor(node: Scan, reader: TableReader) {
    super(node.schema);
    this.scan = node.info;
    this.reader = reader;
  }

  label(): string {
    const { table, version, files, snapshotFiles, columns } = this.scan;
    const pruned = files.length === snapshotFiles.length ? "" : `, ${files.length}/${snapshotFiles.length} files`;
    return `Scan(${table}@v${version}, ${columns.length} col(s)${pruned})`;
  }

  reading(): SourceInput {
    return { kind: "files", files: this.scan.files, bytes: (f) => this.readBytes(f), filters: this.runtimeFilters };
  }

  /** A Parquet reader fetches only the scan's column chunks: the file's size, shared
   * out by column count. */
  readBytes(f: DataFile): number {
    const total = Math.max(f.stats.size, this.scan.columns.length, 1);
    return Math.floor((f.sizeBytes * this.scan.columns.length) / total);
  }

  instance(me: Charged): SourceInstance {
    /** Its split's file rows, and the ns decoding them took. */
    let loaded: { rows: Row[]; decodeNs: number } | null = null;
    let pending: Batch | null = null;
    let filesOpened = 0, rowsRead = 0, bytesRead = 0;
    return {
      prepare: async (split: Split) => {
        if (split.kind !== "file" || !this.scan.columns.length || loaded) return;
        const bytes = await this.reader.readFile(this.scan.meta, split.file);
        const t0 = performance.now();
        const rows = await decodeParquet(bytes, this.scan.columns);
        loaded = { rows, decodeNs: Math.round((performance.now() - t0) * 1e6) };
      },
      open: (ctx: ExecContext, split: Split) => {
        const f = (split as FileSplit).file;
        const cols = this.scan.columns.length;
        let rows: Row[];
        if (cols) {
          const bytes = this.readBytes(f);
          ctx.readStorage(bytes, me);
          const data = loaded;
          if (!data) throw internal(`split ${split.id} was not prepared`);
          rows = data.rows;
          ctx.cpuTaken(data.decodeNs, me);
          loaded = null;
          filesOpened++;
          bytesRead += bytes;
          ctx.cpu(rows.length * cols * COST.scanValue, me);
          ctx.note(me, `read ${f.path}: ${rows.length.toLocaleString("en")} rows, ${cols} column(s), ${bytes.toLocaleString("en")} bytes`);
        } else {
          rows = Array.from({ length: f.rowCount }, () => []);
          ctx.note(me, `${f.path}: ${f.rowCount.toLocaleString("en")} rows from the catalog, not opened`);
        }
        rowsRead += rows.length;
        pending = rows.length ? new Batch(this.output, rows) : null;
      },
      next: () => {
        const b = pending;
        pending = null;
        if (b) me.metrics.gaveOut(b);
        return b;
      },
      state: () => ({ files_opened: filesOpened, rows_read: rowsRead, bytes_read: bytesRead }),
    };
  }

  override describe(): Record<string, unknown> {
    return {
      table: this.scan.table, version: this.scan.version, columns: this.scan.columns,
      files_total: this.scan.snapshotFiles.length, files_pruned: this.scan.snapshotFiles.length - this.scan.files.length,
      runtime_filters: this.runtimeFilters.map((rf) => rf.label), metadata_only: !this.scan.columns.length,
    };
  }
}
