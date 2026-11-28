// The layers' boundaries (ADR 0001, criterion 4): no file under src/cluster,
// src/execution, src/api, src/sql or src/storage imports from src/engines. The shared
// layers know engines only through execution/engine.ts; a Database is given its
// engines by whoever makes it (src/node.ts, the workbench's worker). Also: storage
// does not import sql (a layer below it), and kostka's domain does not import the
// cluster (infrastructure): only its adapters, sim.ts and unit-context.ts, do. The
// two engines never import each other, and the Spark-style engine's domain (physical/,
// driver/, executor/) does not import the cluster either: only its sim.ts does (ADR
// 0002, criterion 10).
import assert from "node:assert/strict";
import { test } from "node:test";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const src = join(root, "src");

function files(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((d) => (d.isDirectory() ? files(join(dir, d.name)) : /\.tsx?$/.test(d.name) ? [join(dir, d.name)] : []));
}

/** Every module a file imports (static, dynamic and re-exports), resolved to a path. */
function imports(file: string): string[] {
  const text = readFileSync(file, "utf8");
  const specs = [...text.matchAll(/(?:import|export)\s[^'"]*?from\s*["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)|^import\s+["']([^"']+)["']/gm)].map((m) => m[1] ?? m[2] ?? m[3] ?? "");
  return specs.filter((s) => s.startsWith(".")).map((s) => resolve(dirname(file), s));
}

test("the shared layers do not import an engine", () => {
  const engines = join(src, "engines");
  const bad: string[] = [];
  let checked = 0;
  for (const layer of ["cluster", "execution", "api", "sql", "storage"]) {
    for (const f of files(join(src, layer))) {
      checked++;
      for (const i of imports(f)) if (i === engines || i.startsWith(engines + "/")) bad.push(`${relative(root, f)} imports ${relative(root, i)}`);
    }
  }
  assert.ok(checked > 25, `checked ${checked} files`);
  assert.deepEqual(bad, []);
});

/** Imports from files under `from` (dirs under src/) into `to` (a dir under src/). */
function crossings(from: readonly string[], to: string): string[] {
  const target = join(src, to);
  return from.flatMap((dir) => files(join(src, dir)).flatMap((f) => imports(f).filter((i) => i === target || i.startsWith(target + "/")).map((i) => `${relative(root, f)} imports ${relative(root, i)}`)));
}

test("storage does not import sql", () => {
  assert.deepEqual(crossings(["storage"], "sql"), []);
});

test("kostka's domain does not import the cluster", () => {
  assert.deepEqual(crossings(["engines/kostka/coordinator", "engines/kostka/worker", "engines/kostka/physical"], "cluster"), []);
  assert.ok(crossings(["engines/kostka"], "cluster").length > 0, "its adapters do: the check sees them");
});

test("the check sees an import of an engine", () => {
  const f = join(src, "node.ts");
  assert.ok(imports(f).some((i) => i.startsWith(join(src, "engines"))), "src/node.ts imports src/engines/index.ts");
});

test("the engines do not import each other", () => {
  assert.deepEqual(crossings(["engines/spark"], "engines/kostka"), []);
  assert.deepEqual(crossings(["engines/kostka"], "engines/spark"), []);
  assert.ok(crossings(["engines/spark"], "core").length > 0, "the check sees the Spark-style engine's imports");
});

test("the Spark-style engine's domain does not import the cluster", () => {
  assert.deepEqual(crossings(["engines/spark/physical", "engines/spark/driver", "engines/spark/executor"], "cluster"), []);
  assert.ok(crossings(["engines/spark"], "cluster").length > 0, "its adapter does: the check sees it");
});

test("both engines get their kernels from core", () => {
  // ADR 0002, criterion 9: the hash table, the accumulators, the row order, the
  // batch and the history live in core/, once. Each engine's operators import them
  // from there, and no engine keeps a copy of its own.
  const core = (k: string) => join(src, "core", `${k}.ts`);
  const hashTable = core("hash-table"), accumulators = core("accumulators"), rowOrder = core("row-order");
  const batch = core("batch"), history = core("history");
  const uses: Record<string, readonly string[]> = {
    "engines/kostka/physical/operators/join.ts": [hashTable, batch],
    "engines/kostka/physical/operators/aggregate.ts": [accumulators, batch],
    "engines/kostka/physical/operators/sort.ts": [rowOrder, batch],
    "engines/kostka/coordinator/state.ts": [history],
    "engines/spark/physical/operators/join.ts": [hashTable, rowOrder, batch],
    "engines/spark/physical/operators/aggregate.ts": [accumulators, batch],
    "engines/spark/physical/operators/sort.ts": [rowOrder, batch],
    "engines/spark/driver/dag-scheduler.ts": [history],
  };
  for (const [file, needed] of Object.entries(uses)) {
    const got = imports(join(src, file));
    for (const k of needed) assert.ok(got.includes(k), `${file} imports ${relative(root, k)}`);
  }
  const copies = files(join(src, "engines")).flatMap((f) => [...readFileSync(f, "utf8").matchAll(/\bclass\s+(HashTable|AggLayout|TopK|Batch|History)\b/g)].map((m) => `${relative(root, f)} declares ${m[1]}`));
  assert.deepEqual(copies, []);
});
