import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { decodeParquet, encodeParquet, parquetRowCount } from "../../src/storage/parquet.ts";
import { columnStats } from "../../src/storage/stats.ts";
import { BloomFilter, HyperLogLog } from "../../src/storage/sketch.ts";
import { ExecutionError } from "../../src/core/errors.ts";
import { daysOf, parseTimestamp } from "../../src/core/types.ts";
import type { ColumnDef } from "../../src/catalog/meta.ts";

const COLS: ColumnDef[] = [{ name: "i", type: "int" }, { name: "f", type: "float" }, { name: "s", type: "string" }, { name: "b", type: "bool" }, { name: "d", type: "date" }, { name: "t", type: "timestamp" }];

describe("Parquet data files", () => {
  test("every type round-trips, nulls included", async () => {
    const rows = [
      [2 ** 53 - 1, 1.5, "zażółć", true, daysOf(1900, 1, 2), parseTimestamp("2024-05-17 13:45:00.123456")!],
      [-(2 ** 53 - 1), -0.25, "", false, daysOf(2100, 1, 1), 0],
      [null, null, null, null, null, null],
    ];
    assert.deepEqual(await decodeParquet(encodeParquet(COLS, rows), COLS.map((c) => c.name)), rows);
  });

  test("reads only the requested columns, in the requested order", async () => {
    const bytes = encodeParquet(COLS, [[1, 2, "x", true, 3, 4]]);
    assert.deepEqual(await decodeParquet(bytes, ["s", "i"]), [["x", 1]]);
    assert.deepEqual(await decodeParquet(bytes, []), [[]]);
    assert.equal(parquetRowCount(bytes), 1);
  });

  test("an empty file", async () => {
    assert.deepEqual(await decodeParquet(encodeParquet(COLS, []), ["i"]), []);
  });

  test("an int64 beyond 2^53 is an error", async () => {
    const bytes = encodeParquet([{ name: "i", type: "int" }], [[1]]);
    // Patch the plain-encoded 1 into 2^60 in place.
    const at = bytes.findIndex((_, k) => bytes[k] === 1 && bytes.slice(k + 1, k + 8).every((x) => x === 0));
    bytes[at + 7] = 0x10;
    await assert.rejects(decodeParquet(bytes, ["i"]), ExecutionError);
  });
});

describe("column statistics and sketches", () => {
  test("min, max, nulls, distinct", () => {
    const s = columnStats("string", ["b", "a", null, "c", "a"]);
    assert.deepEqual([s.min, s.max, s.nullCount, s.distinct], ["a", "c", 1, 3]);
    assert.ok(s.bloom!.mightContain("a") && !s.bloom!.mightContain("zzz"));
    assert.equal(columnStats("bool", [true, null]).sketch, null);
    assert.equal(columnStats("float", Array.from({ length: 2000 }, (_, i) => i + 0.5)).sketch, null);
    assert.ok(columnStats("float", [1.5, 2.5]).sketch);
  });

  test("bloom filters: no false negatives, about 1% false positives", () => {
    const keys = Array.from({ length: 5000 }, (_, i) => `k${i}`);
    const f = BloomFilter.fromBytes(BloomFilter.of(keys, keys.length).toBytes());
    assert.ok(keys.every((k) => f.mightContain(k)));
    let fp = 0;
    for (let i = 0; i < 20000; i++) if (f.mightContain(`other${i}`)) fp++;
    assert.ok(fp / 20000 < 0.02, `${fp / 200}%`);
    assert.ok(!BloomFilter.of([], 0).mightContain("x"));
  });

  test("HyperLogLog estimates within a few percent and unions count overlap once", () => {
    const a = HyperLogLog.of(Array.from({ length: 20000 }, (_, i) => i));
    const b = HyperLogLog.of(Array.from({ length: 20000 }, (_, i) => i + 10000));
    assert.ok(Math.abs(a.estimate() - 20000) / 20000 < 0.06, String(a.estimate()));
    const u = HyperLogLog.fromBytes(a.toBytes());
    u.merge(b);
    assert.ok(Math.abs(u.estimate() - 30000) / 30000 < 0.06, String(u.estimate()));
    assert.equal(new HyperLogLog().estimate(), 0);
    assert.equal(Math.round(HyperLogLog.of([1, 1.0]).estimate()), 1);
  });
});
