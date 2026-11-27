// The block store and the data plane (ADR 0001, criterion 6), with no engine: a block
// of a node that is gone is lost with it.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { BlockLostError, BlockStore, DataPlane } from "../../src/cluster/blocks.ts";
import { rowsBytes } from "../../src/core/rows.ts";

describe("blocks", () => {
  test("a store keeps rows as numbered blocks, with their size", () => {
    const s = new BlockStore(2);
    const a = s.put([[1, "x"]]), b = s.put([[2, "y"], [3, "z"]]);
    assert.deepEqual([a.id, b.id], [0, 1]);
    assert.equal(a.node, 2);
    assert.equal(b.bytes, rowsBytes([[2, "y"], [3, "z"]]));
    assert.equal(s.get(1), b);
    s.drop(0);
    assert.equal(s.get(0), undefined);
    assert.equal(s.size, 1);
  });

  test("the data plane reads a block from its node; a gone node's block is lost", () => {
    const nodes = [{ up: true, blocks: new BlockStore(0) }, { up: true, blocks: new BlockStore(1) }];
    const plane = new DataPlane(nodes);
    const b = nodes[1]!.blocks.put([[7]]);
    assert.deepEqual(plane.read(1, b.id).rows, [[7]]);
    nodes[1]!.up = false;
    assert.throws(() => plane.read(1, b.id), (e: Error) => e instanceof BlockLostError && e.node === 1 && e.block === b.id);
    assert.throws(() => plane.read(0, 99), /does not hold block 99/);
    assert.throws(() => plane.read(5, 0), /no node 5/);
  });
});
