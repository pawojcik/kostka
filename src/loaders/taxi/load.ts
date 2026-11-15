// NYC taxi: a sample of real trips. docs/taxi/trips.parquet holds 100,000 yellow taxi
// trips of January 2024, drawn at random from the 2,964,624 the NYC Taxi & Limousine
// Commission published, in pickup order; docs/taxi/zones.parquet holds its 265 taxi zones.
// Trips outside the month, ending before they start, longer than 6 hours or 100 miles
// (meter errors), or with negative amounts (voided charges) were left out. Columns were
// renamed and payment_type codes spelled out; values are as published. Times are New
// York local time. tools/taxi-sample.ts makes the files.

import type { ColumnDef } from "../../catalog/meta.ts";
import type { ColumnType } from "../../core/types.ts";
import { decodeParquet } from "../../storage/parquet.ts";
import type { OpenTable } from "../tpcds/generate.ts";

const col = (spec: string): ColumnDef[] => spec.trim().split(/,\s*/).map((c) => {
  const [name, type] = c.split(" ");
  return { name: name!, type: type as ColumnType };
});

export const SCHEMA: Record<string, ColumnDef[]> = {
  trips: col(`pickup_time timestamp, dropoff_time timestamp, passengers int, distance float, pickup_zone int,
    dropoff_zone int, payment string, fare float, tip float, tolls float, total float`),
  zones: col("zone_id int, borough string, zone string, service_zone string"),
};
export const TABLES = ["zones", "trips"];
/** Where the files are, relative to the workbench page. */
export const FILES: Record<string, string> = { trips: "taxi/trips.parquet", zones: "taxi/zones.parquet" };

export interface TaxiOptions {
  report?: (table: string, done: number, total: number) => void;
  log?: (text: string) => void;
}

/** Creates the trips and zones tables from the sample's Parquet files. */
export async function loadTaxi(open: OpenTable, files: Record<string, Uint8Array>, o: TaxiOptions = {}): Promise<Record<string, number>> {
  const counts: Record<string, number> = {};
  for (const table of TABLES) {
    const t0 = performance.now();
    const columns = SCHEMA[table]!;
    const rows = await decodeParquet(files[table]!, columns.map((c) => c.name));
    o.report?.(table, 0, rows.length);
    const sink = await open(table, columns, "NYC taxi, January 2024 sample");
    try {
      for (let i = 0; i < rows.length; i += 2048) {
        await sink.add(rows.slice(i, i + 2048));
        o.report?.(table, Math.min(rows.length, i + 2048), rows.length);
      }
      await sink.commit();
    } catch (e) {
      await sink.abort();
      throw e;
    }
    counts[table] = rows.length;
    o.log?.(`${table}: ${rows.length.toLocaleString("en-US")} rows, ${columns.length} columns (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
  }
  return counts;
}
