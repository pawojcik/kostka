// Reading a table version's data files: the port every engine's scan reads Parquet
// files through. The Database implements it over its file store; an engine's scan
// decodes the bytes itself and times that, so decoding counts as the scan's CPU.

import type { DataFile, TableMeta } from "../catalog/meta.ts";

export interface TableReader {
  /** The bytes of `file`, a data file of `table`. Throws TableNotFoundError if the
   * table's files are gone. */
  readFile(table: TableMeta, file: DataFile): Promise<Uint8Array>;
}
