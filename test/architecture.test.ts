// The layers' boundaries (ADR 0001, criterion 4): no file under src/cluster,
// src/execution, src/api, src/sql or src/storage imports from src/engines. The shared
// layers know engines only through execution/engine.ts; a Database is given its
// engines by whoever makes it (src/node.ts, the workbench's worker).
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

test("the check sees an import of an engine", () => {
  const f = join(src, "node.ts");
  assert.ok(imports(f).some((i) => i.startsWith(join(src, "engines"))), "src/node.ts imports src/engines/index.ts");
});
