// kostka's engine as the registry sees it: its settings are checked when a query
// starts, and it plans as many workers as the cluster has nodes.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { KostkaError } from "../../../src/core/errors.ts";
import { kostka, kostkaSettings } from "../../../src/engines/kostka/engine.ts";

describe("kostka's engine", () => {
  test("its settings", () => {
    assert.deepEqual(kostkaSettings({ shufflePartitions: 4, joinStrategy: "shuffle", runtimeFilters: false }), { shufflePartitions: 4, joinStrategy: "shuffle", runtimeFilters: false });
    assert.deepEqual(kostkaSettings({ shufflePartitions: "auto", joinStrategy: undefined }), { shufflePartitions: "auto" });
  });

  test("a wrong setting fails with what it must be", () => {
    const fails = (given: Record<string, unknown>, re: RegExp) => assert.throws(() => kostkaSettings(given), (e: Error) => e instanceof KostkaError && re.test(e.message));
    fails({ shufflePartitions: 0 }, /shufflePartitions must be "auto" or a whole number of at least 1, got 0/);
    fails({ shufflePartitions: 2.5 }, /shufflePartitions must be .* got 2.5/);
    fails({ shufflePartitions: "many" }, /shufflePartitions must be .* got "many"/);
    fails({ joinStrategy: "sideways" }, /joinStrategy must be one of auto, broadcast, shuffle, got "sideways"/);
    fails({ runtimeFilters: "yes" }, /runtimeFilters must be true or false, got "yes"/);
    fails({ workers: 2 }, /kostka has no setting "workers": its settings are shufflePartitions, joinStrategy and runtimeFilters/);
  });

  test("its kinds of step; a unit of work is a split processed", () => {
    assert.equal(kostka.name, "kostka");
    assert.deepEqual(kostka.unitSteps, ["process"]);
    assert.ok(kostka.unitSteps.every((k) => kostka.steps.some((s) => s.kind === k && s.by === "node")));
    assert.deepEqual(kostka.steps.filter((s) => s.bad).map((s) => s.kind), ["split_failed", "worker_gone", "failed"]);
    assert.equal(new Set(kostka.steps.map((s) => s.kind)).size, kostka.steps.length);
  });
});
