// Cuts a sample of NYC TLC yellow taxi trips into kostka-written Parquet files.
// The source files are https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
// and https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv (see src/loaders/taxi/load.ts).
// usage: node tools/taxi-sample.ts <trips.parquet> <zones.csv> docs/taxi 100000
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { zstdDecompressSync } from "node:zlib";
import { parquetMetadata, parquetReadObjects } from "hyparquet";
import { encodeParquet } from "../src/storage/parquet.ts";
import { Random } from "../src/loaders/random.ts";
import { parseCsv } from "../src/loaders/csv.ts";

const [tripsPath, zonesPath, out, want] = process.argv.slice(2) as [string, string, string, string];
const buf = readFileSync(tripsPath);
const file = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
const total = Number(parquetMetadata(file).num_rows);
const compressors = { ZSTD: (input: Uint8Array) => new Uint8Array(zstdDecompressSync(input)) };
const parsers = { timestampFromMicroseconds: (us: bigint) => Number(us), timestampFromMilliseconds: (ms: bigint) => Number(ms) * 1000, timestampFromNanoseconds: (ns: bigint) => Number(ns / 1000n), dateFromDays: (d: number) => d };
const cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "tip_amount", "tolls_amount", "total_amount"];
const PAYMENT: Record<number, string> = { 0: "flex fare", 1: "card", 2: "cash", 3: "no charge", 4: "dispute", 5: "unknown", 6: "voided" };
const FIRST = Date.UTC(2024, 0, 1) * 1000, END = Date.UTC(2024, 1, 1) * 1000;
const rnd = new Random("nyc-taxi-2024-01");
const p = (Number(want) * 1.08) / total;
const rows: unknown[][] = [];
let dropped = 0;
for (let start = 0; start < total; start += 500_000) {
  const objs = (await parquetReadObjects({ file, columns: cols, rowStart: start, rowEnd: Math.min(total, start + 500_000), compressors, parsers } as never)) as Record<string, any>[];
  for (const o of objs) {
    if (rnd.random() >= p) continue;
    const pu = o.tpep_pickup_datetime, du = o.tpep_dropoff_datetime;
    const ok = pu >= FIRST && pu < END && du >= pu && du - pu < 6 * 3600e6 && o.total_amount >= 0 && o.fare_amount >= 0 && o.trip_distance <= 100;
    if (!ok) { dropped++; continue; }
    const n = (v: any) => (v === null || v === undefined ? null : Number(v));
    rows.push([pu, du, n(o.passenger_count), n(o.trip_distance), n(o.PULocationID), n(o.DOLocationID), PAYMENT[Number(o.payment_type)] ?? "unknown", n(o.fare_amount), n(o.tip_amount), n(o.tolls_amount), n(o.total_amount)]);
  }
}
// Keep exactly `want` rows, spread over the month, then order by pickup time.
const keep = rnd.shuffle(rows).slice(0, Number(want)).sort((a, b) => (a[0] as number) - (b[0] as number) || (a[1] as number) - (b[1] as number));
const tripCols = [["pickup_time", "timestamp"], ["dropoff_time", "timestamp"], ["passengers", "int"], ["distance", "float"], ["pickup_zone", "int"], ["dropoff_zone", "int"], ["payment", "string"], ["fare", "float"], ["tip", "float"], ["tolls", "float"], ["total", "float"]].map(([name, type]) => ({ name: name!, type: type as never }));
mkdirSync(out, { recursive: true });
writeFileSync(`${out}/trips.parquet`, encodeParquet(tripCols, keep as never));

const csv = parseCsv(readFileSync(zonesPath, "utf8"));
console.log("zones header", csv[0]);
const zones = csv.slice(1).filter((r) => r.length >= 4 && r[0] !== "").map((r) => [Number(r[0]), r[1], r[2], r[3]]);
writeFileSync(`${out}/zones.parquet`, encodeParquet([{ name: "zone_id", type: "int" }, { name: "borough", type: "string" }, { name: "zone", type: "string" }, { name: "service_zone", type: "string" }] as never, zones as never));
console.log(`total ${total}, sampled ${rows.length + dropped}, dropped ${dropped}, kept ${keep.length}, zones ${zones.length}`);
