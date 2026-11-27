// The cluster's settings: nodes and cores per node, and `workers`, the old name of
// nodes, which must agree with nodes if both are given.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { DEFAULT_CLUSTER, clusterConfig } from "../../src/cluster/config.ts";

describe("cluster settings", () => {
  test("the defaults: one node with one core", () => {
    assert.deepEqual(clusterConfig(), DEFAULT_CLUSTER);
    assert.equal(DEFAULT_CLUSTER.nodes, 1);
    assert.equal(DEFAULT_CLUSTER.cores, 1);
  });

  test("workers is another name for nodes", () => {
    assert.equal(clusterConfig({ workers: 3 }).nodes, 3);
    assert.equal(clusterConfig({ workers: 3, nodes: 3 }).nodes, 3);
    assert.throws(() => clusterConfig({ workers: 3, nodes: 2 }), /workers \(3\) and nodes \(2\) disagree/);
  });

  test("nodes and cores: whole numbers from 1 to 64", () => {
    assert.equal(clusterConfig({ cores: 4 }).cores, 4);
    for (const cores of [0, 65, 1.5]) assert.throws(() => clusterConfig({ cores }), new RegExp(`cores must be 1 to 64, got ${cores}`));
    for (const nodes of [0, 65, 2.5]) assert.throws(() => clusterConfig({ nodes }), new RegExp(`nodes must be 1 to 64, got ${nodes}`));
  });
});
