// A node's memory and disk (ADR 0001, criterion 6), with no engine: blocks spill oldest
// first, state that cannot spill fails with OutOfMemoryError, a full disk with
// OutOfDiskError, and each error ends with the advice given.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { OutOfDiskError, OutOfMemoryError } from "../../src/core/errors.ts";
import { MemoryPool, type SpillSink } from "../../src/cluster/memory.ts";
import { BlockStore } from "../../src/cluster/blocks.ts";

/** A unit that records what it spilled. */
const sink = () => {
  const s = { written: [] as number[], spillWrite(b: number) { s.written.push(b); } };
  return s satisfies SpillSink;
};

describe("MemoryPool", () => {
  test("holds and releases, and keeps its peak", () => {
    const m = new MemoryPool("N0", 100);
    m.hold(sink(), "a", "hash", "a hash table", 60, null, "advice");
    assert.ok(m.has("a"));
    m.release("a");
    assert.equal(m.memory, 0);
    assert.equal(m.peakMemory, 60);
    assert.throws(() => { m.hold(sink(), "b", "hash", "b", 1, null, ""); m.hold(sink(), "b", "hash", "b", 1, null, ""); }, /already holds b/);
  });

  test("spills blocks oldest first to make room, and the unit pays for it", () => {
    const store = new BlockStore(0);
    const b1 = store.put([[1]]), b2 = store.put([[2]]);
    const m = new MemoryPool("N0", 100);
    const s = sink();
    m.hold(s, "b1", "output", "B0", 40, b1, "");
    m.hold(s, "b2", "output", "B0", 40, b2, "");
    m.hold(s, "h", "hash", "a hash table", 50, null, "");
    assert.ok(b1.spilled && !b2.spilled);
    assert.deepEqual(s.written, [40]);
    assert.equal(m.memory, 90);
    assert.equal(m.disk, 40);
    assert.equal(m.spills, 1);
    assert.equal(m.bytesSpilled, 40);
    m.release("b1");
    assert.equal(m.disk, 0);
  });

  test("a block that still doesn't fit goes straight to disk", () => {
    const b = new BlockStore(0).put([[1]]);
    const m = new MemoryPool("N0", 10);
    m.hold(sink(), "b", "output", "B0", 50, b, "");
    assert.ok(b.spilled);
    assert.equal(m.memory, 0);
    assert.equal(m.disk, 50);
  });

  test("state that cannot spill and doesn't fit: OutOfMemoryError, ending with the advice", () => {
    const m = new MemoryPool("N3", 100);
    assert.throws(() => m.hold(sink(), "h", "hash", "a hash table", 200, null, "Give the workers more memory."),
      (e: Error) => e instanceof OutOfMemoryError && e.message.startsWith("N3 ran out of memory: a hash table") && e.message.endsWith("Give the workers more memory."));
  });

  test("a full disk: OutOfDiskError, ending with the disk advice", () => {
    const b = new BlockStore(0).put([[1]]);
    const m = new MemoryPool("N0", 10, 20, "Give the nodes more disk.");
    assert.throws(() => m.hold(sink(), "b", "output", "B0", 50, b, ""), (e: Error) => e instanceof OutOfDiskError && e.message.endsWith("Give the nodes more disk."));
  });

  test("grow: a held state gets bigger, spilling blocks if it must", () => {
    const b = new BlockStore(0).put([[1]]);
    const m = new MemoryPool("N0", 100);
    m.hold(sink(), "b", "output", "B0", 50, b, "");
    m.hold(sink(), "g", "groups", "groups", 20, null, "");
    m.grow(sink(), "g", 60);
    assert.ok(b.spilled);
    assert.equal(m.memory, 60);
  });

  test("state(): what is held, grouped by label, as plain data", () => {
    const store = new BlockStore(0);
    const m = new MemoryPool("N0", 100);
    m.hold(sink(), "x", "output", "B0", 10, store.put([[1]]), "");
    m.hold(sink(), "y", "output", "B0", 15, store.put([[2]]), "");
    m.hold(sink(), "z", "hash", "a hash table", 5, null, "");
    const st = m.state();
    assert.deepEqual(st.memory.held, [{ kind: "output", label: "B0", bytes: 25, count: 2 }, { kind: "hash", label: "a hash table", bytes: 5, count: 1 }]);
    assert.equal(st.memory.used, 30);
    assert.deepEqual(structuredClone(st), st);
  });
});
