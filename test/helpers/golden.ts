// A query run, reduced to what must not change when the code around the engine moves:
// its steps (kind, text, simulated ms) and its result rows, as a count and a SHA-256
// digest, so the fixture stays small. The cases are the SQL-vs-SQLite queries on the
// first three configurations (1, 3 and 4 workers), with CPU time from the cost model
// so that every run takes the same simulated time. Data file names are random, so each
// is replaced by its order of first appearance ("file-1.parquet").
import { createHash } from "node:crypto";
import type { Value } from "../../src/core/types.ts";
import { CONFIGS, SORTED, UNSORTED } from "./sql-cases.ts";

export interface GoldenRun {
  readonly steps: number;
  readonly digest: string;
}

/** One run: its steps and rows, digested. */
export function goldenOf(steps: readonly { readonly kind: string; readonly text: string; readonly ms: number }[], rows: readonly (readonly Value[])[]): GoldenRun {
  const h = createHash("sha256");
  const files = new Map<string, string>();
  const named = (text: string) => text.replace(/data-\d+-[0-9a-f]+\.parquet/g, (f) => {
    if (!files.has(f)) files.set(f, `file-${files.size + 1}.parquet`);
    return files.get(f) ?? f;
  });
  for (const s of steps) h.update(JSON.stringify([s.kind, named(s.text), s.ms]) + "\n");
  h.update(JSON.stringify(rows));
  return { steps: steps.length, digest: h.digest("hex") };
}

/** The queries and configurations the fixture covers, keyed "config index: sql". */
export const GOLDEN_CASES: readonly { readonly key: string; readonly sql: string; readonly config: (typeof CONFIGS)[number] }[] =
  CONFIGS.slice(0, 3).flatMap((config, i) => [...SORTED, ...UNSORTED].map((sql) => ({ key: `${i}: ${sql}`, sql, config })));

export const GOLDEN_FIXTURE = new URL("../fixtures/golden-kostka.json", import.meta.url);
