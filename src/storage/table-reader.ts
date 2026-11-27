// Reading a table version's data files: the port every engine's scan reads Parquet
// files through. The Database implements it over its file store; an engine's scan
// decodes the bytes itself and times that, so decoding counts as the scan's CPU.

import type { DataFile } from "../catalog/meta.ts";
import type { ScanInfo } from "../sql/plan.ts";

export interface TableReader {
  readFile(scan: ScanInfo, file: DataFile): Promise<Uint8Array>;
}
