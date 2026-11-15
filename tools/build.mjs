// Builds the workbench into docs/ (served by GitHub Pages): the page's bundle
// (app.js), the engine's worker (worker.js), sqlite-wasm's binary, the stylesheet
// and index.html. `--watch` rebuilds on every change; `--serve` also serves docs/ on
// http://127.0.0.1:8765 (the workbench needs http: workers and OPFS don't run on file://).

import * as esbuild from "esbuild";
import { copyFile, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("..", import.meta.url));
const out = join(root, "docs");
const serve = process.argv.includes("--serve");
const watch = serve || process.argv.includes("--watch");

const common = {
  bundle: true,
  format: "esm",
  target: "es2022",
  platform: "browser",
  sourcemap: watch,
  minify: !watch,
  // plan nodes and rules are named by their class (constructor.name)
  keepNames: true,
  jsx: "automatic",
  jsxImportSource: "preact",
  logLevel: "info",
  // node-only modules the browser build never reaches
  external: ["node:*"],
  define: { __BUILD__: JSON.stringify(Date.now().toString(36)) },
};

async function statics() {
  await copyFile(join(root, "node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm"), join(out, "sqlite3.wasm"));
  const css = await readFile(join(root, "src/ui/style.css"), "utf8");
  await writeFile(join(out, "style.css"), css);
  const app = await readFile(join(out, "app.js")).catch(() => Buffer.from(""));
  const v = createHash("sha1").update(css).update(app).digest("hex").slice(0, 8);
  await writeFile(join(out, "index.html"), `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>kostka workbench</title>
<meta name="description" content="A small OLAP engine for learning: SQL over Parquet files, run step by step on a simulated cluster in your browser.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<link rel="stylesheet" href="style.css?v=${v}">
</head>
<body>
<div id="app"></div>
<script type="module" src="app.js?v=${v}"></script>
</body>
</html>
`);
}

const plugin = { name: "statics", setup(b) { b.onEnd(async (r) => { if (!r.errors.length) await statics(); }); } };
const builds = [
  { ...common, entryPoints: [join(root, "src/ui/main.tsx")], outfile: join(out, "app.js") },
  { ...common, entryPoints: [join(root, "src/ui/worker.ts")], outfile: join(out, "worker.js"), plugins: [plugin] },
];

if (watch) {
  const contexts = await Promise.all(builds.map((b) => esbuild.context(b)));
  for (const c of contexts) await c.watch();
  if (serve) {
    const { port } = await contexts[0].serve({ servedir: out, host: "127.0.0.1", port: 8765 });
    console.log(`serving docs/ on http://127.0.0.1:${port}`);
  }
} else {
  for (const b of builds) await esbuild.build(b);
}
