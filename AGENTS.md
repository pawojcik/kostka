# Working on kostka

kostka is a small OLAP engine for learning: SQL over Parquet files, compiled and
optimized, then planned and run by an engine on a simulated cluster (kostka's own:
pipelines, a coordinator and workers), with a browser workbench in `docs/`. Its purpose is to be understood, so clarity beats cleverness and
speed. `README.md` is the spec: the layout, every phase and the workbench.

Keep this file short and true: if you find something here stale or wrong, fix it.

## Principles

- **Modules by purpose:** parser, analyzer, optimizer, planner, coordinator, worker,
  storage. Not by kind (no `utils/`, `types/`, `helpers/` grab bags).
- **Narrow and deep:** a module hides a lot behind a small interface.
- **Depend on interfaces:** a module uses another only through its minimal public
  interface, never its implementation details. Layers import only from layers above
  them (Layout in `README.md`).
- **Onion architecture:** domain objects in the middle, physical infrastructure (IO,
  storage, transport, threads, clocks, the simulation) outside, behind interfaces the
  domain defines. In an engine (`engines/kostka/`), the coordinator and workers are
  the domain: they talk only by commands and events, share no mutable objects, and
  know nothing of time or transport; `cluster/` and `engines/kostka/sim.ts` are
  infrastructure that real IO could replace. The shared layers never import an
  engine: they use `execution/engine.ts`.
- **Domain objects are unit-testable** without the infrastructure (no files, no
  simulation).
- No abstraction until a second use needs it.

## Decisions (ADRs)

A significant change (a new concept, protocol, module boundary, or a change to how the
engine works) starts with an ADR in `adr/NNNN-short-name.md`: one goal each, with its
design (Mermaid diagrams), falsifiable exit criteria, alternatives considered, and the
paths it **Affects**. The `plan-work` skill writes one, with its issue;
`implement-adr` implements the issue, and `review-adr` reviews it under the issue. An
ADR is **Proposed** until the user accepts it, then **Accepted** until a later one
supersedes it (say so in both).

Don't read ADRs by default. Before changing code, `grep -l <path> adr/` and read the
ones that concern it.

## Workflow

- Work on `main` unless asked otherwise. Commit and push only when asked; asking for
  an issue to be implemented (`implement-adr`) is asking for its commit and push.
- Before a significant change, agree on a proposed ADR first. Small fixes need none.
- Leave `data/` and `.venv/` alone. Delete only what you were asked to delete.
- No new dependencies without asking.
- Done means: the type check and `npm test` pass, `README.md` (and a tutorial in
  `src/ui/app/tutorial.tsx`, if it shows what changed) is updated in the same change,
  and workbench changes were checked in a browser (load data on the Tables tab first).

## Commands

- `npm test`: all tests (`node --test`, TypeScript run directly by Node 24+).
- `npx tsc --noEmit -p . --noUnusedLocals --noUnusedParameters`: the type check to pass.
- `npm run build`: builds the workbench into `docs/` (minified). The built files are
  not committed: on every push to `main`, `.github/workflows/deploy.yml` tests, builds
  and deploys `docs/` to GitHub Pages.
- `npm run dev`: rebuilds on change and serves http://localhost:8765 (unminified).

## Code and tests

- TypeScript, strict (`noUncheckedIndexedAccess`, `erasableSyntaxOnly`: no enums, no
  parameter properties). Imports end in `.ts`.
- Every file starts with a comment saying what it is and how it works. Comments explain
  why, in full sentences.
- Tests mirror `src/` in `test/`. Query results are checked against SQLite
  (`node:sqlite`); a change to execution adds its configuration to
  `test/api/sql-vs-sqlite.test.ts` and a recovery test (stop a worker at many steps,
  expect the same result). Compare times with the cost model (`cpu: "model"`).

## Writing

The README, ADRs, comments, the workbench's text and errors are read by someone
learning. Write plain, concrete English: short sentences, everyday words. Name things
as the code does (pipeline, split, output, buffer, owner). Errors say what happened
and what to do.
