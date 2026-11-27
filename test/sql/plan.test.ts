// planIds: the numbers the workbench's plan trees and an engine's operators both give
// a plan's nodes: preorder, and a node two parents share is numbered once.
import assert from "node:assert/strict";
import { test } from "node:test";
import { PlanNode, planIds } from "../../src/sql/plan.ts";
import type { Field } from "../../src/core/expr.ts";

class N extends PlanNode {
  readonly schema: readonly Field[] = [];
  readonly name: string;
  readonly kids: readonly PlanNode[];
  constructor(name: string, kids: readonly PlanNode[] = []) {
    super();
    this.name = name;
    this.kids = kids;
  }
  children(): readonly PlanNode[] {
    return this.kids;
  }
  withChildren(children: readonly PlanNode[]): PlanNode {
    return new N(this.name, children);
  }
  detail(): string {
    return this.name;
  }
}

test("planIds numbers nodes in preorder, a shared node once", () => {
  const shared = new N("shared", [new N("leaf")]);
  const root = new N("root", [new N("left", [shared]), new N("right", [shared])]);
  const ids = planIds(root);
  assert.deepEqual([...ids].map(([n, id]) => `${id}:${n.detail()}`), ["0:root", "1:left", "2:shared", "3:leaf", "4:right"]);
});
