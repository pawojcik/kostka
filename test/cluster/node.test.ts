// A node's cores and IO channel (ADR 0001, criterion 7), with no engine: units on two
// cores overlap their CPU, but take turns on the IO channel, waiting in a "wait"
// segment.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { IO_PROFILES, ioCost } from "../../src/cluster/config.ts";
import { IoChannel, Node } from "../../src/cluster/node.ts";
import { MemoryPool } from "../../src/cluster/memory.ts";
import { BlockStore } from "../../src/cluster/blocks.ts";

const cloud = IO_PROFILES.cloud!;
const node = (cores: number) => new Node(0, cores, cloud, new MemoryPool("N0", 1 << 20), new BlockStore(0));
const MB = 1024 * 1024;

describe("a node", () => {
  test("two CPU-only units on two cores, started together, end when one alone would", () => {
    const alone = node(2).begin(0, 0);
    alone.cpu(5e6);
    const one = alone.end(true);
    const n = node(2);
    const a = n.begin(0, 0), b = n.begin(1, 0);
    a.cpu(5e6);
    b.cpu(5e6);
    const ta = a.end(true), tb = b.end(true);
    assert.equal(ta.end, one.end);
    assert.equal(tb.end, one.end);
    assert.deepEqual(n.clocks, [one.end, one.end]);
    assert.equal(n.runs, 2);
    assert.equal(n.time.cpu, 10e6);
  });

  test("two units that each read 1 MB from storage take twice as long; the second waits for the first's IO", () => {
    const io = ioCost(cloud, "storage", MB);
    const n = node(2);
    const a = n.begin(0, 0), b = n.begin(1, 0);
    assert.equal(a.io("storage", MB, "storage_read"), io);
    assert.equal(b.io("storage", MB, "storage_read"), io);
    const ta = a.end(true), tb = b.end(true);
    assert.deepEqual(ta.segments, [["storage", 0, io]]);
    assert.deepEqual(tb.segments, [["wait", 0, io], ["storage", io, 2 * io]]);
    assert.equal(tb.end, 2 * ta.end);
    assert.equal(n.bytes.storage_read, 2 * MB);
    // the wait is not IO time
    assert.equal(n.time.storage, 2 * io);
  });

  test("one core never waits: its units' segments follow each other", () => {
    const n = node(1);
    const a = n.begin(0, 0);
    a.cpu(1e6);
    a.io("network", MB, "network_in");
    a.cpu(1e6);
    const t = a.end(true);
    assert.ok(t.segments.every(([k]) => k !== "wait"));
    for (let i = 1; i < t.segments.length; i++) assert.equal(t.segments[i]![1], t.segments[i - 1]![2]);
    assert.equal(t.end, t.segments[t.segments.length - 1]![2]);
  });

  test("its rules: one unit per core at a time, not before the core's clock, ended once", () => {
    const n = node(1);
    const a = n.begin(0, 10);
    assert.throws(() => n.begin(0, 10), /running a unit already/);
    a.cpu(5);
    a.end(true);
    assert.throws(() => a.end(true), /ended already/);
    assert.throws(() => a.cpu(1), /has ended/);
    assert.throws(() => n.begin(0, 0), /before its clock/);
    assert.throws(() => n.begin(1, 20), /no core 1/);
    assert.throws(() => node(0), /at least one core/);
  });

  test("a failed unit takes its time but does not count as a run", () => {
    const n = node(1);
    const u = n.begin(0, 0);
    u.cpu(3e6);
    u.end(false);
    assert.equal(n.runs, 0);
    assert.equal(n.time.cpu, 0);
    assert.equal(n.clocks[0], 3e6);
  });

  test("frame(): plain data with each core's clock", () => {
    const n = node(2);
    const u = n.begin(1, 0);
    u.cpu(2e6);
    u.end(true);
    const f = n.frame();
    assert.deepEqual(f.cores, [{ clock_ms: 0 }, { clock_ms: 2 }]);
    assert.equal(f.clock_ms, 2);
    assert.equal(f.status, "up");
    assert.deepEqual(structuredClone(f), f);
  });
});

describe("the IO channel", () => {
  test("books the first gap long enough, from the time asked", () => {
    const c = new IoChannel();
    assert.equal(c.book(0, 10, 0), 0);
    assert.equal(c.book(20, 10, 0), 20);
    assert.equal(c.book(0, 10, 0), 10); // fits between 10 and 20
    assert.equal(c.book(0, 5, 0), 30); // no gap left before 30
    assert.equal(c.book(50, 5, 0), 50);
  });
});
