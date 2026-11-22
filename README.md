# kostka

*kostka* is Polish for a small cube (and a die): the OLAP cube, kept small enough to
learn from.

A small analytical database for learning how query engines work. Tables are
versioned sets of immutable Parquet files, described by a SQLite catalog. Queries
are SQL `SELECT`s, compiled into a typed logical plan, optimized by rules, planned
into pipelines and run step by step by a coordinator and workers, Trino-style,
on a **simulated cluster**. Everything runs in the browser (the workbench, in `docs/`) and
in Node (tests, scripts).

```ts
import { openDatabase } from "./src/engine/node.ts";

const db = openDatabase();                       // in memory; openDatabase("data") keeps it
await db.createTable("foo", [["a", "int"], ["b", "int"], ["s", "string"]]);
await db.append("foo", [{ a: 11, b: 2, s: "x" }, { a: 3, b: 4, s: "y" }]);
const r = await db.query("select a + b as c from foo where a > 10");
// r.columns: [{ name: "c", type: "int" }], r.rows: [[13]]
```

TypeScript run directly by Node (type stripping, Node 24 or later). Dependencies:
`hyparquet` / `hyparquet-writer` (Parquet), `@sqlite.org/sqlite-wasm` (the catalog in
the browser; Node uses the built-in `node:sqlite`), Preact and CodeMirror 6 (the
workbench). `npm test` runs the tests, `npm run typecheck` the type checker,
`npm run build` builds the workbench into `docs/`.

## Layout

The code is split by phase. Each layer only imports from the layers above it in this
list (the UI and `engine/` import everything).

```
src/
  core/         types and values, errors, bound expressions, functions, hashing
    functions/  the function registry, scalar functions and operators, casts, aggregates
  catalog/      table metadata in SQLite: tables, snapshots, files, per-file statistics
  storage/      file stores (memory, Node, OPFS), Parquet data files, statistics
                and sketches, the table writer
  sql/          the compiler front end: lexer, parser, AST, analyzer, logical plan
    optimizer/  rules, cardinality estimates
  physical/     the physical planner, pipelines, output buffers, splits, operators,
                the cost model, the ExecContext operators run in
    operators/  scan, streaming (filter, project, limit), aggregate, join, sort,
                buffer sinks and sources, runtime filters
  execution/    the Execution API, the protocol, cluster settings
    coordinator/  the coordinator, its QueryState and recovery (domain)
    worker/     workers: split queues, output buffers, the cache, memory (domain)
    sim/        the simulation: transport, clocks, data plane, run context, and the
                Execution built from them, SimExecution (infrastructure)
  loaders/      CSV import, column generators, NYC taxi, SSB and TPC-DS data and queries
  engine/       Database (tables + loaders + queries), CompiledQuery, Node setup
  ui/           the workbench: Session (engine side, in a Web Worker), RPC, the SQL
                editor, the Preact app
test/           mirrors src/; results are checked against SQLite (node:sqlite)
tools/build.mjs builds the workbench (esbuild) into docs/
docs/           GitHub Pages: the built workbench, plus engine-comparison.html and
                spark-job-lab.html
```

A query goes through:

```
SQL ─parse─▶ AST ─analyze─▶ logical plan ─rules─▶ optimized plan
    ─physical planner─▶ pipelines + output buffers ─coordinator─▶ splits ─workers─▶ result
```

## Cross-cutting semantics

### Types and values

| type | value in a row | notes |
|---|---|---|
| `int` | JS number, a safe integer | ±(2^53 − 1); arithmetic that leaves the range fails (`ExecutionError`) |
| `float` | JS number, finite | NaN and ±∞ are errors |
| `string` | JS string | compared by code point |
| `bool` | `true` / `false` | never interchangeable with numbers |
| `date` | number: days since 1970-01-01 | |
| `timestamp` | number: µs since 1970-01-01 00:00 UTC | naive, no time zone |

Every column is nullable (`null`). Rows are arrays of values; what a number means
comes from its column's type, which the analyzer knows for every expression. This is
how integers are emulated over JS numbers: the type, not the value, says a number
is an int, and int operators check their results.

Type names accept aliases (`integer`, `bigint` → int; `double`, `real` → float;
`text`, `varchar` → string; `boolean` → bool).

**Implicit conversions** (widening) are only `int → float` and `date → timestamp`
(midnight). A comparison, `IN`, `CASE`, `COALESCE` or arithmetic finds the common
type of its arguments by widening; anything else is an `AnalysisError`
("'+' expects (int, int) or (float, float), got (string, int)"). A string literal
compared with a date or timestamp is parsed as one at analysis. Every other
conversion is an explicit `CAST`; a cast that cannot exist is an analysis error, a
value that cannot convert (`CAST('x' AS int)`) an execution error.

`int / int` is a float. `%` takes the dividend's sign. Division by zero fails.

### Nulls

Functions and operators return null when an input is null, except `coalesce`,
`is_null`, `is_not_null` and the three-valued `and` / `or` / `not`. A filter (WHERE,
HAVING, ON) keeps a row only when its condition is TRUE. `x IN (…)` is null when x
is null, or when no element matches and one is null. Group keys and DISTINCT treat
nulls as equal; join keys never match a null.

### Identifiers

Table and column names are `[A-Za-z_][A-Za-z0-9_]*`. In SQL, keywords are
case-insensitive and identifiers case-sensitive; `"double quotes"` allow a keyword
as a name. `DATE` and `TIMESTAMP` are keywords only before a string or `(`, so a
table or column can be called `date` without quotes.

### Errors

Every error kostka raises on purpose is a `KostkaError` (`core/errors.ts`):
`CatalogError` (`TableExistsError`, `TableNotFoundError`), `SchemaError` (bad table
definitions or values on load), `SqlError` (`ParseError`, `AnalysisError`, each with
the span of the offending text), `ExecutionError` (`OutOfMemoryError`),
`CommitConflictError`. Anything else is a bug (`internal()`).

### Row order

Row order is part of the result **only when the query ends in an ORDER BY**, and
then only by its keys: rows with equal keys may come out in any order. Without one,
the order of rows, groups and join matches depends on which worker ran which split
when. LIMIT without ORDER BY keeps *some* n rows.

## SQL (`sql/`)

One `SELECT` statement:

```
query     := [WITH name [(cols)] AS (query), ...] select
select    := SELECT [DISTINCT] item, ... [FROM from] [WHERE expr]
             [GROUP BY expr, ...] [HAVING expr]
             [ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], ...] [LIMIT int]
item      := * | name.* | expr [[AS] alias]
from      := source (, source | [INNER | LEFT [OUTER] | RIGHT [OUTER] | CROSS] JOIN source [ON expr])*
source    := table [FOR VERSION AS OF int | FOR TIMESTAMP AS OF 'ts'] [[AS] alias]
           | (query) [AS] alias [(cols)] | cte name
expr      := literals (int, float, 'string', TRUE/FALSE, NULL, DATE '…', TIMESTAMP '…'),
             columns (name, alias.name), + - * / % ||, = <> != < <= > >=,
             AND OR NOT, IS [NOT] NULL, [NOT] IN (…), [NOT] BETWEEN, [NOT] LIKE,
             CASE [x] WHEN … THEN … [ELSE …] END, CAST(x AS type), f(args),
             aggregates: count(*), count(x), sum, avg, min, max
```

No DDL or DML: tables are created and loaded through the API and the workbench.
Not supported: `count(DISTINCT x)` (use a subquery with `SELECT DISTINCT`),
subqueries in expressions, window functions, set operations, `OFFSET`.

- **Lexer** (`lexer.ts`) and **parser** (`parser.ts`, recursive descent) produce an
  AST (`ast.ts`) whose nodes carry their span in the text.
- **Analyzer** (`analyzer.ts`) resolves every name (tables at one pinned snapshot,
  CTEs, subqueries, columns, `*`), checks types, and builds the logical plan in the
  clauses' logical order: FROM → WHERE → GROUP BY / aggregates → HAVING → SELECT →
  DISTINCT → ORDER BY → LIMIT. GROUP BY and ORDER BY may use select aliases and
  positions, HAVING select aliases; a FROM column of the same name wins over an alias. Aggregates are only allowed in SELECT, HAVING and ORDER BY of a grouped
  query; non-aggregated columns must be group keys. Constant expressions are folded
  (so `CAST('x' AS int)` as a literal is an analysis error). A derived output name is
  the expression's text without balanced outer parentheses.
- Every table is read at **one version for the whole query**: the current one, or
  the one `FOR VERSION / TIMESTAMP AS OF` names.

### Expressions (`core/expr.ts`, `core/compile.ts`)

Bound expressions are typed trees of nodes implementing the `Expression` interface:
`type`, `children()`, `withChildren()`, `compile(ctx)` (its evaluator; the
`CompileContext` resolves column slots and compiles children) and `format(child,
ids)` (its text). The kinds are `Literal`, `ColumnRef`, `Call` (a scalar function
bound to its argument types), `AggCall` (an aggregate, likewise bound), `Case`,
`Cast`, `Alias`, all subclasses of `Expr`; nothing dispatches on the node's class to
compile or print it, so a new kind is one new class. Columns are referenced by
**expression id**, never by position: every scan column and every computed column
gets a fresh id, so rewrites can move expressions between plan nodes without
renumbering. A `Field` (qualifier, name, id, type) describes a column of a node's
output. Operators compile expressions into closures once, resolving ids to row
slots for the layout they receive.

### Functions (`core/functions/`)

A registry of `ScalarFunction`s by name; the parser maps operators to names (`+`,
`=`, `and`, `neg`, `is_null`, `concat`, `in`, `like`, …). A `ScalarFunction` has a
name, category, doc and `resolve(argTypes)`, which binds it to the call's argument
types as a `BoundFunction`: the result type, the types the arguments are cast to,
and `compile(argEvaluators)`. `overloads([...])` builds `resolve` from a table of
signatures, picking the one the call's types match exactly, else widen to; each
gives a null-propagating `impl` (wrapped by `strict`) or its own `compile`. Adding a
function is one `register({...})` call; it then shows up in completion and the
reference.

Built in: arithmetic and comparisons; `and or not`; `coalesce nullif`; strings
`upper lower length trim ltrim rtrim contains starts_with ends_with replace
substring/substr concat`; dates `to_date to_timestamp date_add date_diff date_trunc
year month day hour minute day_of_week minutes_between`; numbers `abs round floor ceil`.

Aggregates (`aggregates.ts`) are `AggregateFunction`s whose `resolve(argType)` gives
a `BoundAggregate`, defined by a **state**: a fixed list of typed values with
`init`, `add`, `merge` and `result`. Because they run in two phases the
state travels through an output buffer like any other columns. Built in: `count`,
`sum`, `avg`, `min`, `max`. Adding one is a `registerAggregate({...})`.

### Logical plan (`plan.ts`)

Immutable nodes, each with its output `schema`:

```
Scan(table, snapshot, columns, files)   reads one pinned snapshot
Filter(child, condition)
Project(child, exprs)
Aggregate(child, keys, aggs)            output = keys, then aggregates
Join(left, right, kind, condition)      inner, left, cross (the analyzer turns a right
                                        join into a left join with the sides swapped)
Sort(child, keys)
Limit(child, n)
TopN(child, keys, n)                    Sort + Limit, made by a rule
```

`Distinct` is an Aggregate with keys only.

### Optimizer (`optimizer/`)

A `Rule` (`name`, `doc`, `apply(plan) → {plan, changed}`) rewrites a plan into an
equivalent one: the same output columns and rows (and their order, if the plan
sorts). `optimize` runs each rule to a fixpoint, in order, recording every changed
plan and the time of every pass for the workbench. A query can run any subset of
the rules; with none, the analyzed plan runs as written.

1. **filter_pushdown**: moves filters toward the scans: through projections
   (substituting computed columns), sorts, into the matching side of joins (inner
   joins both sides; outer joins only the preserved side), below aggregates for
   conjuncts on group keys; merges adjacent filters, turns a cross join plus an
   equality into an inner join.
2. **file_pruning**: drops the files of a scan that its filter cannot keep a row of,
   judged from per-file min/max, null counts and bloom filters. The filter stays.
3. **join_reorder**: for each region of inner/cross joins, dynamic programming over
   connected subsets of its leaves (bushy trees allowed), minimizing the summed
   estimated intermediate rows; the smaller side becomes the build (right) side.
4. **projection_pushdown**: every node keeps only the columns someone above uses;
   scans read only those columns.
5. **topn**: `Limit(Sort)` → `TopN`.

**Cardinality** (`cardinality.ts`) estimates rows and distinct counts from the
catalog's statistics of the files a scan still reads: row counts, null counts,
min/max (one histogram bucket per file) and merged HyperLogLog sketches; standard
selectivities for equality, ranges, LIKE and IN; join sizes from distinct counts.
Errors of 2× are normal.

## Catalog (`catalog/`)

Table metadata in SQLite, behind a small synchronous `SqlDb` interface so the same
code runs on `node:sqlite` and on sqlite-wasm in a browser worker (OPFS, SAH-pool
VFS, so no COOP/COEP headers are needed).

```
tables        id, name, dir (its folder in the file store)
snapshots     table, version, created_us, operation, detail, schema (JSON)
files         path, rows, bytes, added_in version, removed_in version (or null)
file_columns  per-file column statistics: min, max, nulls, distinct, HLL, bloom
settings      key -> JSON value (file_rows)
```

- A **commit** is one SQLite transaction that appends a snapshot, adds files and
  marks removed ones. It is optimistic: if a file it removes is no longer live, it
  fails with `CommitConflict` and the writer retries from the new snapshot.
- A snapshot's files are those added at or before it and not removed at or before
  it. Nothing is purged, so every version stays readable.
- **Revert** adds a new snapshot listing an old version's files again.
- `created_us` is strictly increasing per table, so `FOR TIMESTAMP AS OF` is exact.
- Data files are not the catalog's business: writers write them first, and a
  dropped table's folder is deleted by the caller.

## Storage (`storage/`)

- **FileStore**: async read/write/delete/list of relative paths
  (`<table dir>/<file>`), with atomic writes. In memory (tests, a page without OPFS),
  a local folder (Node), the browser's origin-private file system (OPFS, in the
  worker, through sync access handles).
- **Data files** are Parquet with one row group (`parquet.ts`, hyparquet):
  int → INT64, float → DOUBLE, string → UTF8, bool → BOOLEAN, date → INT32 DATE,
  timestamp → INT64 TIMESTAMP(MICROS). Readers decode only the columns they need.
  An INT64 outside ±2^53 is an error.
- **Statistics** (`stats.ts`, `sketch.ts`), computed when a file is written: min, max,
  null count, exact distinct count, a HyperLogLog sketch (P = 10, ~3% error; for a
  float column only when the file has at most 1,024 distinct values), and for
  strings a bloom filter (~1% false positives).
- **TableWriter** (`writer.ts`): `create`, `replace` (new schema, only the new files;
  history stays) or `append` (the current schema). It takes rows in any number of
  `add` calls, writes a file each time it holds `file_rows` rows (a setting, default
  8,192), and commits all of them as **one snapshot**. Append compacts: live files
  smaller than `file_rows` are read and rewritten with the new rows, so every file
  but the last is full. Tables are loaded, not edited: no update or delete.

## Loaders (`loaders/`)

- **CSV** (`csv.ts`): RFC 4180 parsing (quoted delimiters, quotes and newlines);
  each column's type inferred from all its values (the first of int, float, bool,
  date, timestamp every non-null value parses as; else string). The caller can
  rename, retype or drop columns before the rows are written (create, replace or
  append). Options: delimiter, header, null values, a decimal separator (`.` or `,`),
  a thousands separator (none, `,`, `.`, space, `'`; groups of three digits), and
  date and timestamp format patterns (`formatParser`: YYYY YY, MMM MM M, DD D, HH H,
  hh h with A for AM/PM, mm, ss, S… for fractions, Z for an offset; other characters
  literal; ISO 8601 when empty).
- **Generators** (`generators.ts`, `random.ts`): seeded (sfc32) value generators per
  column (sequences, ranges, choices, names, dates, …), with defaults from the
  column's name and type. The same seed gives the same rows everywhere.
- **NYC taxi** (`taxi/`): real data, shipped with the workbench as two Parquet
  files in `docs/taxi/` (2.2 MB) and fetched when loaded: 100,000 yellow taxi trips of
  January 2024 sampled at random from the TLC trip records (trips), in pickup order,
  and the 265 taxi zones (zones). `tools/taxi-sample.ts` cuts the sample from the
  published files. 8 example queries with SQLite references.
- **SSB** (`ssb/`): the Star Schema Benchmark's lineorder fact table and its four
  dimensions (date, customer, supplier, part) with seeded data following the SSB
  domains: yyyymmdd date keys, 25 nations in 5 regions with 10 cities each, 5
  manufacturers × 5 categories × 40 brands (every brand has parts), integer money
  columns. lineorder is 6M × scale rows in order-date order; the dimensions have
  minimum sizes so the selective queries return rows at small scales. The 13 standard
  queries (flights 1-4) are the examples, with plain-language explanations.
- **TPC-DS** (`tpcds/`): the 24 tables of the TPC-DS schema with every column,
  filled with seeded data whose keys and domains follow the specification closely
  enough for the standard queries to return rows (it is not dsdgen). Row counts are
  scale × the SF 1 counts (date_dim and time_dim are always full). 13 example
  queries follow TPC-DS queries 1, 3, 7, 15, 19, 34, 43, 55, 65, 93, 96, 99 and a
  shuffle-heavy one; where kostka's SQL needs another shape (q1 uses a CTE instead
  of a correlated subquery) the original is kept as `reference` and the tests check
  both give the same rows on SQLite.

## Physical planning (`physical/`)

The **physical planner** (`planner.ts`) turns an optimized plan into **pipelines**
(`source → operator* → sink`) joined by **output buffers**. It reads no data.

```
Scan               ScanSource starts a pipeline
Filter, Project    appended to the current pipeline
Limit              appended where the pipeline reads one split; else a local Limit,
                   a gather that needs n rows, and a final Limit reading it whole
Join               the build (right) side ends in an output buffer:
                   - broadcast (no keys, or (workers − 1) × build ≤ probe by estimate,
                     or forced): the probe side continues the current pipeline
                   - shuffle: both sides are shuffled by key into N partitions; a new
                     pipeline reads the probe side's partitions
                   HashProbeOp joins; an inner join can add runtime filters
Aggregate          PartialAggregateSink shuffles partial states by key (gathers them
                   for a global aggregate); MergeAggregateSource starts the next pipeline
Sort / TopN        SortSink / TopNSink write sorted runs to a gather buffer;
                   MergeSortSource merges them
root               ResultSink
```

Planner settings: `shufflePartitions` (N, or the default "auto": 2 × workers), `joinStrategy` (auto,
broadcast, shuffle), `runtimeFilters` (on). A new logical node is supported by
`registerPlanner(kind, fn)`.

### Operators (`operator.ts`)

A `PhysicalOperator` is an immutable **spec**: input and output `Field`s, a
`label()`, a JSON `describe()` of its configuration, and the output buffers it reads
or writes. The plan is what every worker gets; nothing in it changes while a query
runs. A worker creates an **instance** of each operator **per split** and drives
it:

```
source.open(ctx, split)
while (batch = source.next(ctx)) if (!head.push(ctx, batch)) break
head.finish(ctx)        passed down the chain: the end of the split, sinks flush
```

**A split is self-contained**: no operator state outlives it. Whatever a sink holds
(partial groups, a sort buffer, a top-n heap, rows not yet written) is written when
its split ends, so everything a split produced is known when it is done, and a
split can be run again on another worker and produce the same output. The one thing
a worker keeps across splits is a **cache**: hash tables built from a build side
(read-only, and always rebuildable from their buffer). `push` returning false ends
the split early (a full Limit). A source instance may load what a split needs first
(`prepare`, async: reading a data file). Instances report what they held or did with
`state()`.

To add an operator: subclass `Operator`, `Source` or `Sink`, implement `instance()`
and `describe()`, give a source its `reading()` (what the coordinator makes splits
from), and emit it from the planner.

Operators never see workers, clocks or other splits. They report the work they do
to the **ExecContext** (`context.ts`): modelled CPU, bytes read from storage, memory
they hold, pieces they write to the pipeline's output buffer, pieces they fetch,
keys they collected for a runtime filter, result rows; the infrastructure places it
in time.

- **Scan**: one split per data file; reads only its columns; storage time for its
  share of the file's bytes. A scan with no columns (`count(*)`) opens no file.
- **Filter, Project**: streaming.
- **Limit**: counts within its split. Where a pipeline has several splits, the
  planner makes it two-phase: a local Limit per split, a **gather** buffer that
  needs only n rows (`enough`), and a final Limit in a pipeline that reads the
  gather as one split. Once the gather holds n rows the coordinator gives out no
  more of the producer's splits.
- **Aggregate**: `PartialAggregateSink` folds a split's rows into groups
  (first-seen order) and at the end of the split writes each group's key and states
  to its partition. `MergeAggregateSource` reads one **whole** partition per split
  (a group's states must meet in one place) and emits the results. A global
  aggregate gathers into one partition, read even when empty.
- **Join** (`HashProbeOp`): the build side is a buffer the probe split reads whole:
  every piece (broadcast) or those of the split's partition (shuffle). The worker
  builds the table the first time a split needs it and keeps it in its cache
  (`ctx.cache`) until the coordinator releases the build buffer; other splits of
  that partition on that worker reuse it. Inner, left (a probe row with no match
  gets nulls) and cross joins. A join can multiply rows, so it emits its output in
  batches of at most batch rows, never one array for a whole input batch.
- **Runtime filters**: an inner join's build splits collect their keys and report
  them with `SplitDone`; when the build pipeline is done the coordinator merges
  them, skips the probe scan's files whose min/max cannot hold a key, and sends the
  keys with the probe pipeline's splits, which drop rows right after the scan.
- **Sort / TopN**: each split sorts its rows (or keeps its best n in a heap) and
  writes one sorted run to a gather buffer; `MergeSortSource` reads every run as
  one split and merges them k-way, stopping after n rows for a TopN.

### Output buffers, outputs and splits (`buffer.ts`)

- **Output buffer**: where a pipeline's sink writes, for the pipelines after it.
  Mode shuffle (N partitions by a stable hash of the key, `hash.ts`, the same in
  Node and every browser), broadcast (one partition, read whole by every reader)
  or gather (one partition, read as one split). The buffer belongs to the
  pipeline, but its contents are **partial and spread out**: each worker holds
  what the splits it ran wrote.
- **Piece**: rows of one partition written at once (a batch's worth, or the rest
  at the end of the split), kept by the worker that wrote it in memory, or on its
  disk once spilled, until the buffer is released. Others know a piece only by its
  **PieceRef**: id, buffer, partition, worker, split, rows, bytes.
- **Output**: what one split wrote to one partition: `(split, partition)`, its
  pieces, rows and bytes. Outputs are the coordinator's unit of lineage: it knows
  which split made every output, and which splits read it.
- **Partitioning**: the buffers whose partition p must meet in one place share a
  partitioning (a shuffle join's build and probe sides). The coordinator assigns
  each partition of a partitioning to one worker, its **owner**; every split of p
  goes there.
- **Split**: the unit of work, of scheduling and of recovery. A **file split** is
  one live data file of a scanned table. A **buffer split** is (buffer, partition,
  outputs it reads). A split knows its rows and bytes and where its data is
  (shared storage, or the outputs it reads), never the rows themselves. Splits
  name outputs, not pieces: an output made again after a failure has new pieces,
  and the splits reading it do not change.
- When a buffer is sealed, the coordinator **cuts** its reader's splits from the
  outputs: per partition, outputs are joined up to `splitRows`, or all of a
  partition goes into one split for a reader that needs it whole (an aggregate's
  merge, a gather). N is the lever against big partitions.

### CPU time

A cluster setting, `cpu`, chooses where an operator's CPU time comes from:

- **measured** (the default): the time its code actually took. The runtime wraps
  every operator instance; the run context keeps a stack of the operators running
  and charges the time between two events (an operator called or returning, IO
  charged) to the one on top, so an operator gets its own time, not that of the
  operators it pushes rows to. Decoding a data file is async, so the scan times it
  while the file loads and charges it when its split runs. Measured time varies
  between runs, and so may the schedule.
  Browsers coarsen `performance.now()` (to 100 µs without cross-origin isolation,
  which GitHub Pages cannot enable), so very short calls are noisy; their sum is
  still about right.
- **model** (`cost.ts`): computed from rows and rough per-row nanoseconds for a
  columnar engine on one core: scan decode, filter, project, hash insert (≈ 1.2
  probes), hash probe, sort (n log n), merge, output row (≈ 0.3), and so on. Every
  run of a query takes the same simulated time and has the same schedule.

IO time is always computed, from bytes and the IO profile.

## Execution (`execution/`)

Execution is a small distributed system. Its **domain** is the coordinator and the
workers, which talk only by messages; the **infrastructure** around them carries the
messages, keeps time, stores data, runs the work and notices when a worker is gone.
Today the infrastructure is a simulation in one thread; real transport, threads and
IO could replace it without touching the domain.

```
SQL ─parser─▶ analyzer ─▶ optimizer ─▶ physical planner ─▶ coordinator ─commands─▶ workers
                                                                    ◀──events───
```

The model has few concepts: **pipelines** (with the barrier between them), **output
buffers** with their **partitions** and **owners**, **outputs** (what a split wrote
to a partition), **splits** and **workers**. There are no tasks: every worker has
the whole plan and can run a split of any pipeline. "P1 on W0" is just the splits
of P1 that W0 runs.

### The domain

- **Coordinator** (`coordinator/`), one per query. It owns the **QueryState** and is
  the only one that knows the whole query; it never reads rows (except the result):
  - workers: which are up, and which are gone (`ClusterState`);
  - pipelines: waiting, running, done (and running again while it remakes lost
    outputs);
  - splits: status (pending, queued, done, skipped, failed), the worker it went to,
    attempts, why; a pending split is **blocked** while an output it reads is not
    available;
  - output buffers: every output (split, partition, pieces, rows, bytes, worker)
    and whether it is available, lost or released; status empty, filling, sealed,
    released;
  - partition owners per partitioning; runtime filter keys; per-operator metrics
    from the split reports; the result rows, by split.
- **Workers** (`worker/`): each has the plan, a queue of splits per pipeline, its
  pieces of every output buffer, its cache of hash tables and its memory. A worker
  decides spilling (pieces first, oldest first; state that cannot fit fails the
  split with `OutOfMemoryError`, and a piece that doesn't fit on its disk with
  `OutOfDiskError`). It runs one split at a time, the one that
  arrived first.

Commands (coordinator → worker) and events (→ coordinator), `protocol.ts`:

| command | |
|---|---|
| `AddSplits` | splits of one pipeline for its queue, at most `k` = 2 queued per pipeline; with the pieces of each output they read, the pieces of the build sides they read whole, and the pipeline's runtime filter keys |
| `ReleaseBuffer` | free its pieces of a buffer and the hash tables built from it: every reader is done |

| event | |
|---|---|
| `SplitDone` | a split ran: the pieces it wrote (its outputs), the runtime filter keys it collected, result rows, and a report (time, per-operator metrics) |
| `SplitFailed` | the error; or that a piece it read was gone (its worker was), so it can run again later |
| `WorkerGone` | from the infrastructure, not a worker: a worker stopped, with everything it held |

Everything a split did reaches the coordinator in one `SplitDone`, so a split is
either known to be done, with all its outputs, or not done at all.

### The flow

1. **Submit**: the coordinator gets the physical plan and the workers; every worker
   has the plan.
2. **Pipeline ready** (every pipeline it depends on is done): the coordinator makes
   its splits: data files (skipping files the runtime filters rule out), or splits
   cut from the sealed input buffer's outputs. For a buffer read it assigns the
   partitioning's partitions to workers (most bytes held first, balanced).
3. **Assignment**: the coordinator fills each worker's queue for the pipeline up to
   k = 2 with `AddSplits`: file splits to any worker (a fair share at first),
   offered to the least loaded workers first (by the rows of splits queued on
   them across every pipeline, so independent pipelines of a split or two each
   spread out instead of piling on W0), a partition's splits to its owner. A worker pulls nothing: it runs what is
   queued.
4. **`SplitDone`**: the coordinator records the outputs and refills the queue. A
   worker with nothing left gets a partition that has not started on its owner
   (no stealing of started partitions): the partition moves.
5. **Pipeline done** (every split done or skipped): its output buffer is sealed;
   runtime filter keys are merged; buffers every reader is done with are released
   (`ReleaseBuffer`); waiting pipelines may become ready. The query is done when
   every pipeline is.
6. A gather that has **enough** rows for a Limit: the producer's pending splits
   are skipped. A **failure** (`SplitFailed` with an error) stops the query.

### Recovery

When a worker is gone (`WorkerGone`; no new workers are provisioned) the
coordinator recovers from **lineage**, without restarting the query:

1. The worker's queued splits go back to pending.
2. Its outputs are **lost**. An output is still **needed** if a split that is not
   done reads it (or, for a build side, the partition still has probe splits to
   run); a released output is needed again the same way. The split that made a
   needed output runs again (pending, attempt + 1), and its own pipeline is
   running again; a split that runs again may need outputs that are lost or
   released, and so on back to the scans. Outputs nobody needs stay lost.
3. Partitions it owned move to the workers left (balanced); the new owner builds
   its hash tables again from the build side.
4. Splits queued on other workers that read a lost piece fail with `SplitFailed`
   (a lost piece), and go back to pending.
5. A pending split is blocked until every output it reads is available again; when
   the outputs are made again (new pieces, same `(split, partition)`) it runs.
6. Result rows are kept by split, so a split's rows are never counted twice. The
   query fails when no worker is left.

### Infrastructure: the simulation (`sim/`, `cluster.ts`)

Ports the domain uses, and their simulated adapters:

- **Transport**: commands reach a worker at once; events go through a queue ordered
  by simulated time, so the coordinator handles them in the order they happened.
- **Executor and clock**: one thread per worker. A worker runs a split when it has
  one, starting at the later of its clock and when the split arrived; the
  simulation always advances the earliest thing that can happen (an event to
  deliver, or a split to start). A split runs atomically and is placed on its
  worker's timeline as segments of CPU, storage read, network, spill write and
  spill read.
- **Data plane**: a worker fetches a piece from the worker holding it: network time
  if it is another worker, disk time if it was spilled; an error if that worker is
  gone.
- **Failures**: `stopWorker(w)` stops a worker at the current time: its events not
  yet delivered are lost with it, and the coordinator gets `WorkerGone`.
- **Table storage**: data files are read from shared storage (storage time).
- **Settings**: workers (1–8), memory per worker (default 2 MB), disk per worker
  (default 16 MB), an IO profile (cloud, fast, slow, none: bandwidth and latency of
  shared storage, local disk and network), batch rows (the size of a batch, and of
  the pieces a sink writes; default 4,096), split rows, CPU time (measured or
  model). The small memory default makes the larger examples spill now and then.

### The Execution API (`runtime.ts`)

A compiled query's `start()` creates an `Execution` (`SimExecution`: the
coordinator, the workers and the simulation above):

```ts
interface Execution {
  readonly done: boolean; readonly elapsedNs: number;
  step(): Promise<boolean>;      // the next step
  wave(): Promise<number>;       // steps until each worker ran one split
  runConcurrent(): Promise<void> // until the pipelines running now are done
  run(): Promise<Row[]>;
  stopWorker(w: number): void;   // a failure, now: WorkerGone
  result(): Row[];
  readonly events: readonly ExecEvent[];  // every step so far
  state(at?: number): Record<string, unknown>;  // the workbench's view after step `at`
  progress(): { done: number; total: number };  // splits
}
```

Nothing runs until it is stepped. The history is the message log, grouped into
**steps** (`ExecEvent`: seq, simulated ms, kind, text, the worker, pipeline, split,
buffer and partition it concerns, and its messages). Splits are the units of work,
so steps are about splits:

| step | messages |
|---|---|
| `start` | the query is submitted; a plan estimated to produce more than a billion rows at any node (`MAX_PLAN_ROWS`) is refused at once: the next step is `failed`, naming the node, and for a cross join, `filter_pushdown` |
| `plan` | a pipeline is ready: its splits are made (files, or cut from outputs), partitions get owners |
| `assign` | `AddSplits` to a worker |
| `process` | a worker ran a split: `SplitDone` (or `SplitFailed`) |
| `split_done` | the coordinator handles a `SplitDone`: outputs, filter keys, result rows; it refills the worker's queue |
| `split_failed` | the coordinator handles a `SplitFailed`: the split goes back to pending (it read a lost piece) |
| `pipeline_done` | buffer sealed, filters merged, `ReleaseBuffer`s |
| `enough` | a gather holds enough rows for its Limit: splits skipped |
| `worker_gone` | `WorkerGone`, and the recovery it starts |
| `done`, `failed` | |

`state(at)` is the query right after any step, with no cap: what the coordinator
knows (workers, pipelines, splits, outputs, owners, filters, operator metrics, what
it decided and why) next to what each worker holds (memory, disk, pieces, cached
tables, its clock and time). What only grows (splits, outputs, units, pieces) keeps
the step it appeared at, values that change keep their history by step, and the
rest is kept per step as a frame that shares what the step did not touch with the
frame before.

## Engine (`engine/`)

- **CompiledQuery** (`query.ts`): SQL → logical (analyzer) → optimized (rules, with
  the trace and timings) → physical plan → an Execution.
  `phases` records the real time of parse, analyze, optimize and physical planning.
  Options: `rules`, `cluster`, `planner`. The physical plan is
  immutable: every `start()` runs it afresh on a new coordinator and workers.
- **Database** (`database.ts`), on any `SqlDb` and `FileStore`:
  tables (`listTables`, `describe`, `files`, `history`, `createTable`, `dropTable`,
  `writer`, `append`, `replace`, `revert`, the `file_rows` setting), loaders
  (`inferCsv`, `importCsv`, `defaultGenerators`, `generate`, `loadSample`,
  `loadTpcds`, `loadSsb`, `loadTaxi`) and queries (`compile`, `query` → `{columns, rows}`).
- `openDatabase(folder?)` (`node.ts`): the catalog in `node:sqlite`, data files in the
  folder; in memory without one.

## Workbench (`ui/`, built into `docs/`)

A single page; the engine runs in a **Web Worker** so loads and queries never block
it. Catalog and data files live in OPFS, so tables survive a reload; where OPFS is
not available (another tab holds it, a private window) everything is in memory.

- **Session** (`session.ts`) answers the page with JSON-friendly values: reference
  data (`info`: storage, rules, cluster options, types, generators, TPC-DS scales,
  examples, functions;
  `schema` for completion), tables (`tables`, `table` at any version, create, drop,
  revert, settings), loaders with progress events, and queries: `check(sql)` (parse
  and analyze for the editor), `compile`, `step(from)` (the next step after
  `from`, running the simulation only when that step has not happened yet),
  `next(from)` (the next split processed), `wave`, `finish`, `run`,
  `stopWorker(w)` (a failure at the latest step), `state(at)` (after any step),
  `close`. Answers carry the state, its step and the steps the
  page does not have yet.
- **RPC** (`rpc.ts`, `worker.ts`): `{id, name, args}` → `{id, ok, value | error}`, with
  progress `{id, event}`; calls run one at a time.
- **SQL editor** (`editor.ts`): CodeMirror 6 with completion of tables, columns
  (with their types, through aliases) and functions (with signatures) from the
  catalog, and squiggles under parse and analysis errors as you type.
- **Tutorials** (`app/tutorial.tsx`): a Tutorial button opens four guided scenarios:
  your first query (NYC taxi), how the work is split (SSB Q2.1: broadcast and shuffle
  joins, splits, partitions), a worker fails (NYC taxi on 3 workers: lost outputs,
  recovery, then every worker stopped) and what the optimizer saves you (SSB Q4.1 with
  rules turned off: refused cross joins, then the cost of reading every column). Each
  loads its data if needed and opens its query; a callout shows one step at a time and
  a ring marks the element it is about (`data-tour` attributes; a control inside the
  closed Options is marked by Options). A step asks for at most one action, and the
  step after an action points at what it did (the plan, the banner, the timeline, the
  result, a stopped worker, the failure). Steps that ask for an action move on once
  the reader has done it, judged from the query's state and a count
  of the controls used; the reader always uses the real controls.
- **App** (`app/`, Preact): a Tables tab (load CSV, NYC taxi, SSB, TPC-DS or a sample, create empty
  tables; a table's rows at any version, history and revert, data files with their
  statistics, a generator) and a tab per query: the SQL, the options to compile
  with in five groups (optimizer rules; physical planner: join distribution, shuffle
  partitions, runtime filters; cluster: workers, memory, disk, IO speeds; data flow:
  batch and split size; simulation: CPU time), Compile / Step / Next split / Wave / Finish / Run, a
  history slider over every step, a banner saying what the step shown did, and
  views: plan (as a tree joined by lines, by stage: the analyzed plan, then the
  plan after each rule; a rule that changed nothing is marked), program (a graph
  of pipelines and output buffers joined by lines along which data flows, left to
  right on a wide screen and top to bottom on a narrow one; an operator with 10%
  or more of the operators' time so far carries a heavy icon, and the heavy
  operators are listed under the graph at every step; clicking a pipeline, an
  operator or a buffer highlights its lines and shows its state in a popover next
  to it (Escape or a click elsewhere closes it); the view opens on the pipeline
  that matters at the step shown),
  **query state** (what the coordinator sees, drawn rather than listed: the
  workers, up or gone, with their queues (a split run again shows its attempt);
  each pipeline's splits as a grid colored by status and worker; each output
  buffer as partitions × outputs, by holder, with owners and lost outputs;
  lineage: hover a split or an output to see what it reads and what reads it;
  runtime filters), workers (a timeline lane per worker, time by category, memory,
  disk and cached tables, and a button to stop a worker), steps (the list,
  filtered by kind, each with its commands and events; click one to go to it),
  trace (one Gantt tree on one time axis from the start of parsing: parse,
  analyze, each optimizer pass and physical planning in real time, then the
  simulated execution from where compilation ended, with a line marking the switch;
  execution goes down to pipelines and then workers, each split a bar in its
  pipeline's color, of 20)
  and result. All of them
  show the query as it was right after the step chosen.

`npm run dev` rebuilds on every change and serves `docs/` on http://127.0.0.1:8765
(unminified, with source maps: run `npm run build` before committing). `npm run build` bundles `src/ui/main.tsx` → `docs/app.js` and `src/ui/worker.ts` →
`docs/worker.js` (esbuild, class names kept: plan nodes are named by their class),
and copies `sqlite3.wasm`, the stylesheet and `index.html`.

## Tests (`test/`)

`node --test`, mirroring `src/`. Query results are checked against SQLite
(`node:sqlite`) on the same rows: hand-written queries over small tables, and every
NYC taxi, SSB and TPC-DS example on 1, 4 and 8 workers, ignoring row order unless the query sorts.
Also: functions and casts (including int overflow), the parser and analyzer (errors
and their spans), each optimizer rule, execution (the protocol's order, partition
owners and moves, cutting splits from outputs, self-contained splits, limits,
spilling, out of memory, recovery from a stopped worker at every step of a query,
with the same result), the catalog (commits,
conflicts, time travel, revert), storage (Parquet round trips, statistics, sketches),
the writer (compaction, retries), CSV, NYC taxi loading, SSB and TPC-DS generation, and the Session.
