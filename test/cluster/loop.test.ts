// The event loop and the history (ADR 0001, criterion 6), with no engine: it always
// does the earliest thing that can happen (an event first on a tie), a stopped node's
// undelivered events are lost, and the history shares frames of nodes a step did not
// touch.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { IO_PROFILES } from "../../src/cluster/config.ts";
import { Node } from "../../src/cluster/node.ts";
import { MemoryPool } from "../../src/cluster/memory.ts";
import { BlockStore } from "../../src/cluster/blocks.ts";
import { EventLoop, History, type Runnable } from "../../src/cluster/loop.ts";

const nodes = (n: number) => Array.from({ length: n }, (_, i) => new Node(i, 1, IO_PROFILES.none!, new MemoryPool(`N${i}`, 1e6), new BlockStore(i)));

/** A loop over `n` nodes where each node has `work[i]` units of `ns` each, and every
 * unit sends an event when it ends. Returns what happened, in order. */
function harness(n: number, work: number[], ns: number) {
  const ns_ = nodes(n);
  const left = [...work];
  const log: string[] = [];
  const loop: EventLoop<string, Runnable> = new EventLoop<string, Runnable>(ns_, {
    runnable: () => ns_.filter((x) => x.up && (left[x.id] ?? 0) > 0).map((x) => ({ node: x.id, core: 0, arrived: 0 })),
    run: async (r, start) => {
      const u = ns_[r.node]!.begin(r.core, start);
      u.cpu(ns);
      const t = u.end(true);
      left[r.node]!--;
      log.push(`run N${r.node} @${start}`);
      loop.send(t.end, r.node, `done N${r.node} @${t.end}`);
    },
    deliver: (e, at) => log.push(`deliver ${e} (at ${at})`),
  });
  return { loop, log, nodes: ns_ };
}

describe("the event loop", () => {
  test("does the earliest thing first; an event before a unit on a tie", async () => {
    const { loop, log } = harness(2, [2, 1], 10);
    while (await loop.advance());
    assert.deepEqual(log, [
      "run N0 @0", "run N1 @0",
      "deliver done N0 @10 (at 10)", "deliver done N1 @10 (at 10)",
      "run N0 @10", "deliver done N0 @20 (at 20)",
    ]);
    assert.equal(loop.elapsed, 20);
    assert.equal(await loop.advance(), false);
  });

  test("a stopped node: it runs nothing more, and its undelivered events are lost", async () => {
    const { loop, log, nodes: ns } = harness(2, [2, 2], 10);
    await loop.advance(); // N0 runs
    const { at, lost } = loop.stop(0, 0);
    assert.equal(at, 0); // N1 could start at 0: that is when N0 stops
    assert.deepEqual(lost, [{ at: 10, event: "done N0 @10" }]);
    assert.equal(ns[0]!.up, false);
    while (await loop.advance());
    assert.ok(log.slice(1).every((l) => !l.includes("N0")), log.join("\n"));
    assert.throws(() => loop.stop(0, 0), /gone already/);
    assert.throws(() => loop.stop(7, 0), /no node 7/);
  });
});

describe("the history", () => {
  test("a frame of every node after each step; untouched nodes share the frame before", () => {
    const ns = nodes(2);
    let made = 0;
    const h = new History(ns, (n) => ({ status: n.up ? "up" as const : "gone" as const, node: n.id, made: made++ }));
    h.record({ ms: 0, kind: "start", text: "", node: null, refs: {}, messages: [] }, new Set());
    const s1 = h.record({ ms: 1, kind: "work", text: "", node: 1, refs: { unit: 3 }, messages: [] }, new Set([1]));
    assert.equal(s1.seq, 1);
    assert.equal(h.framesAt(1)[0], h.framesAt(0)[0]);
    assert.notEqual(h.framesAt(1)[1], h.framesAt(0)[1]);
    ns[0]!.up = false;
    h.record({ ms: 2, kind: "gone", text: "", node: 0, refs: {}, messages: [] }, new Set());
    assert.equal(h.framesAt(2)[0]!.status, "gone", "a node gone since gets a new frame");
    assert.equal(h.framesAt(2)[1], h.framesAt(1)[1]);
    assert.throws(() => h.framesAt(9), /no step 9/);
  });
});
