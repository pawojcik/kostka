// The kernels every engine's operators share (ADR 0002): the hash table, aggregate
// state in two phases, row order (comparing keys, the best k rows, a k-way merge), a
// value step by step, and a row's width from its column types.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { AggLayout, type Group } from "../../src/core/accumulators.ts";
import { AggCall, ColumnRef, SortKey } from "../../src/core/expr.ts";
import { lookupAggregate } from "../../src/core/functions/aggregates.ts";
import { HashTable, keyOf } from "../../src/core/hash-table.ts";
import { History } from "../../src/core/history.ts";
import { TopK, bestRun, keyComparator, type Keyed } from "../../src/core/row-order.ts";
import { ROW_OVERHEAD, fieldsBytes, typeBytes } from "../../src/core/rows.ts";
import type { Row, Value } from "../../src/core/types.ts";

const col = (i: number) => (r: Row) => r[i]!;

describe("core kernels", () => {
  test("HashTable: rows by key; a null key never matches; no keys is one bucket", () => {
    const t = new HashTable();
    t.add([[1, "a"], [2, "b"], [1, "c"], [null, "d"]], [col(0)]);
    assert.equal(t.rows, 3);
    assert.equal(t.nullKeys, 1);
    assert.deepEqual(t.buckets.get(1), [[1, "a"], [1, "c"]]);
    assert.ok(t.bytes > 0);
    assert.equal(keyOf([null, 1], [col(0), col(1)]), undefined);
    assert.equal(keyOf([5], []), 0);
    assert.notEqual(keyOf([1, "2"], [col(0), col(1)]), keyOf(["1", 2], [col(0), col(1)]));
  });

  test("AggLayout: fold rows (partial), merge partial rows (final), results", () => {
    const call = (name: string) => {
      const fn = lookupAggregate(name)!;
      return new AggCall(fn, new ColumnRef(1, "v", "int"), fn.resolve("int"));
    };
    const sum = call("sum"), avg = call("avg");
    const layout = new AggLayout([sum, avg]);
    const args = [col(1), col(1)];
    const a: Group = { key: ["x"], states: layout.init() };
    for (const r of [["x", 1], ["x", 2], ["x", null]] as Row[]) layout.add(a.states, args, r);
    const b: Group = { key: ["x"], states: layout.init() };
    layout.add(b.states, args, ["x", 6]);
    const merged: Group = { key: ["x"], states: layout.init() };
    for (const g of [a, b]) layout.merge(merged.states, layout.partial(g), 1);
    assert.deepEqual(layout.result(merged), ["x", 9, 3]);
    assert.equal(layout.partial(a).length, 1 + layout.width);
  });

  test("row order: ASC/DESC and nulls; the best k; a k-way merge", () => {
    const cmp = keyComparator([new SortKey(new ColumnRef(0, "a", "int"), false, false)]);
    const sorted = ([[1], [null], [3], [2]] as Value[][]).sort(cmp);
    assert.deepEqual(sorted, [[3], [2], [1], [null]]);
    const asc = keyComparator([new SortKey(new ColumnRef(0, "a", "int"))]);
    const top = new TopK(2, asc);
    for (const v of [5, 1, 4, 2, 3]) top.offer({ k: [v], row: [v] });
    assert.deepEqual(top.take().map((x) => x.row[0]), [1, 2]);
    assert.equal(top.seen, 5);
    const runs: Keyed[][] = [[1, 4], [2, 3]].map((r) => r.map((v) => ({ k: [v], row: [v] })));
    const heads = [0, 0];
    const out: Value[] = [];
    for (let i = bestRun(runs, heads, asc); i >= 0; i = bestRun(runs, heads, asc)) out.push(runs[i]![heads[i]!++]!.row[0]!);
    assert.deepEqual(out, [1, 2, 3, 4]);
  });

  test("History: the value after any step", () => {
    const h = new History(2, "a");
    h.set(5, "b");
    h.set(5, "c");
    assert.equal(h.after(1), undefined);
    assert.equal(h.after(4), "a");
    assert.equal(h.after(9), "c");
    assert.equal(h.since, 2);
  });

  test("fieldsBytes: a row's width from its column types", () => {
    assert.equal(typeBytes("int"), 8);
    assert.equal(typeBytes("bool"), 1);
    assert.equal(typeBytes("string"), 16);
    assert.equal(fieldsBytes([{ type: "int" }, { type: "string" }]), ROW_OVERHEAD + 24);
  });
});
