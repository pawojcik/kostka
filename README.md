# kostka

*kostka* is Polish for a small cube (and a die): the OLAP cube, kept small enough to
learn from.

A small analytical database for learning how query engines work. Tables are
versioned sets of immutable Parquet files, described by a SQLite catalog. Queries
are SQL `SELECT`s, compiled into a typed logical plan and optimized by rules; an
**engine** then plans and runs the optimized plan step by step on a **simulated
cluster** of nodes. kostka's own engine plans pipelines and runs them with a
coordinator and workers, Trino-style; a Spark-style engine cuts the same plan into
stages and runs them as tasks with a driver and executors. Everything runs in the
browser (the workbench, in `docs/`) and in Node (tests, scripts).

```ts
import { openDatabase } from "./src/node.ts";

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
`npm run build` builds the workbench into `docs/`. On every push to `main`, GitHub
Actions (`.github/workflows/deploy.yml`) runs the type check and the tests, and if they
pass, builds the workbench and deploys `docs/` to GitHub Pages. The built files are not
committed.

## Layout

The code is split by phase. Each layer only imports from the layers above it in this
list. The shared layers (everything but `engines/`) know an engine only through
`execution/engine.ts`; `src/node.ts` and the workbench's worker give a Database every
engine there is (`engines/index.ts`), and the UI imports everything. The two engines
never import each other.

```
src/
  core/         types and values, errors, bound expressions, functions, hashing,
                the categories of time, the CPU cost model and row sizes (plain
                models, used by engines' operators and the cluster alike), and the
                kernels every engine's operators share: batches, the hash table,
                aggregate state in two phases (accumulators), row order, and a value
                step by step (history)
    functions/  the function registry, scalar functions and operators, casts, aggregates
  catalog/      table metadata in SQLite: tables, snapshots, files, per-file statistics
  storage/      file stores (memory, Node, OPFS), Parquet data files, statistics
                and sketches, the table writer, the TableReader engines read files with
  sql/          the compiler front end: lexer, parser, AST, analyzer, logical plan
    optimizer/  rules, cardinality estimates
  cluster/      the simulated cluster every engine runs on (infrastructure): settings
                and IO profiles, nodes (cores, clocks, the IO channel), memory and
                disk, blocks and the data plane, the event loop and its history
  execution/    the seam: what an engine is, an Execution, the state every engine
                shows (ExecState), and the registry of engines
  api/          Database (tables + loaders + queries) and CompiledQuery
  engines/      the engines (index.ts lists them)
    kostka/     kostka's engine: engine.ts (its settings and kinds of step)
      physical/   the physical planner, pipelines, output buffers, splits, operators,
                  the ExecContext operators run in
        operators/  scan, streaming (filter, project, limit), aggregate, join, sort,
                  buffer sinks and sources, runtime filters
      coordinator/  the coordinator, its QueryState and recovery (domain)
      worker/   workers: split queues, their pieces, the cache (domain)
      protocol.ts  commands and events
      sim.ts    SimExecution: the coordinator and one worker per node on the cluster
      unit-context.ts  the context a split runs in: time, IO, fetching pieces
    spark/      the Spark-style engine: engine.ts (its settings and kinds of step)
      physical/   the planner, the Spark operators (operators/), the cut into jobs
                  and stages, the TaskContext operators run in
      driver/   the DAGScheduler, the TaskScheduler and the MapOutputTracker (domain)
      executor/ executors: task slots, the block manager, task memory (domain)
      protocol.ts  commands and events
      sim.ts    SparkExecution: the driver and one executor per node on the cluster
  loaders/      CSV import, column generators, the users/orders sample's queries,
                NYC taxi, SSB and TPC-DS data and queries
  node.ts       openDatabase on Node, with every engine
  ui/           the workbench: Session (in a Web Worker), RPC, the SQL editor, the
                Preact app (app/; each engine's own views in app/engines/)
test/           mirrors src/; results are checked against SQLite (node:sqlite)
tools/build.mjs builds the workbench (esbuild) into docs/
docs/           GitHub Pages: the built workbench (not committed), plus the taxi data,
                engine-comparison.html and spark-job-lab.html
.github/        the workflow that tests, builds and deploys to GitHub Pages
adr/            decisions (ADRs)
```

A query goes through:

```
SQL ─parse─▶ AST ─analyze─▶ logical plan ─rules─▶ optimized plan ─engine─▶ an Execution
kostka: optimized plan ─physical planner─▶ pipelines + output buffers
        ─coordinator─▶ splits ─workers─▶ result
spark:  optimized plan ─planner─▶ Spark plan ─cut─▶ jobs and stages
        ─driver─▶ tasks ─executors─▶ result
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

Table and column names are `[A-Za-z_][A-Za-z0-9_]*`. Keywords and names are
case-insensitive, and names are case-preserving (as in DuckDB or Spark):
`select A from FOO f where F.b > 1` reads table `foo` and its columns `a` and `b`,
and the output column is called `a`, as the catalog spells it, unless aliased.
`"double quotes"` allow a keyword as a name and ignore case too. So the catalog
refuses a table `FOO` next to `foo` and a table with columns `id` and `ID`, and an
append matches columns by name ignoring case (`addObjects`, CSV append). The
catalog compares table names with `COLLATE NOCASE` under a unique index; one made
before names ignored case that holds two such tables fails to open, naming them. `DATE` and `TIMESTAMP` are keywords only before a string or `(`, so a
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

## Physical planning (`engines/kostka/physical/`)

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

kostka's settings (a query's `settings`; `planner` is their old name, still accepted)
are the planner's: `shufflePartitions` (N, or the default "auto": 2 × workers),
`joinStrategy` (auto, broadcast, shuffle), `runtimeFilters` (on). The engine checks
them when the query starts: an unknown setting or value fails with a KostkaError. The
number of workers is the cluster's number of nodes. A new logical node is supported by
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
- **model** (`core/cost.ts`): computed from rows and rough per-row nanoseconds for a
  columnar engine on one core: scan decode, filter, project, hash insert (≈ 1.2
  probes), hash probe, sort (n log n), merge, output row (≈ 0.3), and so on. Every
  run of a query takes the same simulated time and has the same schedule.

IO time is always computed, from bytes and the IO profile.

## Execution (`execution/`, `cluster/`, `engines/kostka/`)

A query runs on an **engine** (`execution/engine.ts`). An engine takes the optimized
plan, plans it in its own way and runs it on the shared simulated **cluster**
(`cluster/`): nodes with cores, memory, disk, blocks and an IO channel, an event loop
and a history of steps. The API, the Session and the workbench's shared views see
only what every engine has (an `Execution` and its `ExecState`); each engine's own
objects pass through its state to its own views. A query picks its engine by name
(`engine`: `kostka`, the default, or `spark`); an unknown name fails with
`UnknownEngineError`, which lists the engines there are.

kostka's engine is the rest of this section. Its execution is a small distributed system. Its **domain** is the coordinator and the
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

- **Coordinator** (`engines/kostka/coordinator/`), one per query. It owns the **QueryState** and is
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
- **Workers** (`engines/kostka/worker/`), one per node: each has the plan, a queue
  of splits per pipeline, its pieces of every output buffer and its cache of hash
  tables. A piece's rows are a block of its node, and the worker holds pieces and
  state in its node's memory through interfaces it defines (`NodeMemory`,
  `NodeBlocks`); the node's memory decides spilling (blocks first, oldest first;
  state that cannot fit fails the split with `OutOfMemoryError`, and a block that
  doesn't fit on its disk with `OutOfDiskError`). It runs one split at a time, the
  one that arrived first, on core 0 of its node.

Commands (coordinator → worker) and events (→ coordinator), `engines/kostka/protocol.ts`:

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

### Infrastructure: the cluster (`cluster/`) and the simulation (`sim.ts`)

The cluster is shared by every engine; `engines/kostka/sim.ts` places one worker on
each node and connects the coordinator and the workers to it.

- **Nodes** (`node.ts`): `nodes` simulated machines, each with `cores` cores, its
  memory and disk (`memory.ts`), its blocks (`blocks.ts`) and one **IO channel**
  (its storage, disk and network bandwidth). Each core has its own clock and runs one
  **unit** of work at a time (for kostka, a split). A unit is laid out on its core's
  timeline as segments of CPU, storage read, network, spill write and spill read.
  CPU segments of units on different cores overlap; IO segments take turns on the
  channel, and the time a unit waits for it is a **wait** segment. With one core in
  use nothing waits.
- **Transport and executor** (`loop.ts`, the `EventLoop`): events go through a queue
  ordered by simulated time, so the scheduler (kostka's coordinator) handles them in
  the order they happened; commands reach a node at once. A core runs a unit when it
  has one, starting at the later of its clock and when the unit arrived; the loop
  always does the earliest thing that can happen (an event to deliver first on a
  tie, then a unit to start).
- **History** (`loop.ts`): every step with its messages, and a frame of every node
  right after it (a node the step did not touch shares the frame before).
- **Data plane** (`blocks.ts`): a node reads a block from the node holding it:
  network time if it is another node, disk time if it was spilled; `BlockLostError`
  if that node is gone (kostka's worker sees it as a lost piece).
- **Failures**: `stopNode(n)` stops a node just before the next thing happens: its
  events not yet delivered are lost with it, and kostka's coordinator gets
  `WorkerGone`.
- **Table storage**: data files are read from shared storage (storage time).
- **Settings** (`config.ts`): nodes (1 to 64, the workbench offers 1, 2, 4 and 8;
  `workers` is another name for it), cores per node (1 to 64, default 1; kostka uses
  core 0 only, the Spark-style engine every core), memory per node (default 2 MB),
  disk per node (default 16 MB), an IO profile (cloud, fast, slow, none: bandwidth
  and latency of shared storage, local disk and network), batch rows (the size of a
  batch, and of the pieces a sink writes; default 4,096), split rows, CPU time
  (measured or model). The small memory default makes the larger examples spill now
  and then.

### The Execution API (`execution/engine.ts`, `execution/registry.ts`)

An engine, and what starting a query on it gives:

```ts
interface Engine {
  name: string; label: string;              // "kostka", "kostka (pipelines)"
  steps: StepKindInfo[];                    // its kinds of step: who takes them, labels, docs
  unitSteps: string[];                      // the steps where a unit ran ("process")
  start(q: { optimized, reader, cluster, settings }): Execution;  // plans; runs nothing
}

interface Execution {
  readonly engine: string; readonly done: boolean; readonly elapsedNs: number;
  step(): Promise<boolean>;      // the next step
  wave(): Promise<number>;       // steps until each node ran one unit
  runConcurrent(): Promise<void> // until the work running now is done
  run(): Promise<Row[]>;
  stopNode(n: number): void;     // a failure, now
  result(): Row[]; rows(): readonly Row[];
  explain(): string;             // the engine's physical plan, as text
  readonly events: readonly ExecEvent[];  // every step so far; the first is there at once
  state(at?: number): ExecState;          // the query right after step `at`, plain data
  progress(): { done: number; total: number };
}
```

`ExecState` has what every engine shows: `engine`, `at`, `events`, `event`, `done`,
`failed`, `elapsed_ms`, `next`, `cluster`, `nodes` (each node's frame: status, each
core's clock, time by category, bytes, memory and disk), `units` (each unit on its
node and core: start, end, label, group, failure, segments) and `result_rows`; then
the engine's own objects (kostka: pipelines, splits, buffers, filters, the log; Spark:
operators, joins, jobs, stages, tasks, shuffles, the log).
`Engines` (`registry.ts`) holds the engines a Database was given, by name.

kostka's `SimExecution` is its coordinator, its workers and the simulation above;
`stopWorker(w)` is another name for `stopNode(w)` there.

Nothing runs until it is stepped. The history is the message log, grouped into
**steps** (`ExecEvent`: seq, simulated ms, kind, text, the node it happened on (null
for the scheduler), `refs`: what it concerns in the engine's terms (kostka: pipeline,
split, buffer, partition), and its messages). kostka's splits are its units of work,
so its steps are about splits:

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

## Spark-style engine (`engines/spark/`)

Most people who learn about distributed query engines meet Spark first. This engine
runs the same optimized plan the way Spark does: a plan cut into **stages** at every
shuffle, each stage run as **tasks**, one per partition, in the **task slots** of
executors that use every core of their node. It shares nothing with kostka's engine
but the shared layers: the two never import each other. Comparing them on the same
query and cluster shows what each design costs: stage barriers against pipelines,
shuffle files on disk against output buffers in memory, a broadcast threshold in
bytes against kostka's rule, task retries and stage resubmission against kostka's
recovery from lineage.

### Planning (`physical/planner.ts`)

The planner maps each logical node to Spark's operators (`physical/operators/`), and
adds an **Exchange** wherever a child's partitioning is not what its parent needs, as
Spark's EnsureRequirements does:

```
Scan              FileScan (one partition per data file; one empty one with no files)
Filter, Project   Filter, Project
Aggregate         HashAggregate(partial) → Exchange hashpartitioning(keys, n)
                  → HashAggregate(final); SinglePartition without keys
Join, equi-keys   BroadcastExchange (the right side) + BroadcastHashJoin when the build
                  side's estimated bytes ≤ broadcastThreshold; else a hash Exchange and
                  a Sort on both sides → SortMergeJoin
Join, no keys     BroadcastExchange (the right side) + BroadcastNestedLoopJoin
Sort              Exchange SinglePartition → Sort
TopN              TakeOrderedAndProject(partial) → Exchange SinglePartition
                  → TakeOrderedAndProject(final)
Limit             LocalLimit → Exchange SinglePartition → GlobalLimit
```

An operator that needs one partition gets no Exchange if its child is one already (a
Limit above a Sort). The **estimated build bytes** are the optimizer's row estimate
times the row width of its columns (`core/rows.ts`, `fieldsBytes`: 8 bytes a number,
1 a bool, 16 a string, plus the row overhead). The right side is the build side, and a
left join keeps the left side's unmatched rows, so the right side can always be
broadcast. Operators pull batches from their children (Spark's iterator model);
the ones between two exchanges run in one task, batch by batch: Spark's whole-stage
code generation would compile them into one function, a **WholeStageCodegen** box.
Nothing is generated; the name says what Spark would do.

Settings (a query's `settings`): `broadcastThreshold` (bytes, default 32768, like
Spark's autoBroadcastJoinThreshold of 10 MB scaled to kostka's small nodes; -1: never
broadcast), `shufflePartitions` (`"auto"`, 2 per core in the cluster: nodes × cores ×
2, or a number), `maxTaskFailures` (default 4). The engine checks them when the query
starts. The cluster settings are shared (`ClusterConfig`).

### Jobs, stages and tasks (`physical/stages.ts`)

- A **job** runs one action. The query's result is one job; each BroadcastExchange is
  a job of its own, run before the jobs that read it (independent ones side by side).
- A job is cut into **stages** at each Exchange. A **ShuffleMapStage** ends in one:
  each of its tasks writes one **shuffle file**, its rows grouped by reduce partition
  with an index of where each partition starts (Spark's sort-based shuffle). A
  **ResultStage** ends at the job's output: its tasks send their rows to the driver.
- A stage has one **task** per partition: one per file in a scan stage,
  `shufflePartitions` in one that reads a hash Exchange, one in one that reads a
  SinglePartition one.
- A stage starts only when every parent stage is done: stages are **barriers**, and no
  data flows between them while a parent runs.

### The driver and the executors

The **driver** (`driver/`, domain) reads no rows but the result's and the broadcasts':
- **DAGScheduler**: starts jobs once the broadcasts they read are made, submits a stage
  when its parents' shuffle output is available, records results, frees shuffles and
  broadcasts no stage reads any more, and recovers (below). Its steps are named like
  Spark's listener events.
- **TaskScheduler**: each stage attempt is a task set. It gives tasks to free slots,
  first in first out by job and stage, each to the executor with the most free slots
  (no data locality), and runs a failed task again, up to `maxTaskFailures` attempts.
- **MapOutputTracker**: where each map task's shuffle file is (node, block, bytes per
  reduce partition). Reduce tasks are launched with these locations.

An **executor** (`executor/`, domain), one per node:
- **task slots**: one per core (Spark's spark.task.cpus = 1). Slot *i* runs its task on
  core *i*, so a node's tasks run side by side and share its IO channel.
- **block manager**: the node's shuffle files (on its disk) and the broadcasts it has
  fetched (in its memory). A broadcast is fetched from the driver once per executor and
  built into one hash table, which every task there shares.
- **memory**: each running task may hold a fixed share, memoryMB / cores. A Sort that
  outgrows it spills sorted runs to the local disk and merges them; a HashAggregate
  that does not fit fails the task with OutOfMemoryError.

Commands (driver → executor) and events (→ driver), `protocol.ts`:

| message | |
|---|---|
| `LaunchTask` | stage, attempt, partition, task attempt, slot; where its shuffle input is (the MapOutputTracker's locations); the broadcasts it reads |
| `RemoveShuffle`, `RemoveBroadcast` | free the files or the copy: no stage left reads them |
| `TaskFinished` | a map task's **MapStatus** (its shuffle file), or a result task's rows; metrics |
| `TaskFailed` | an error, or **FetchFailed**: a shuffle file it read is gone |
| `ExecutorLost` | from the infrastructure: a node is gone, with its executor, its running tasks and its shuffle files |

### Recovery

- **A node is gone**: its running tasks run again on other executors; this does not
  count toward `maxTaskFailures`. Its shuffle files are lost, and the driver
  unregisters them. A stage submitted later sees its parent's missing outputs and
  resubmits the parent first; a task of a stage already running fails with
  **FetchFailed**, that stage attempt fails, the parent is resubmitted
  (**StageResubmitted**, attempt 1) for its missing partitions only, and the failed
  stage runs again after it. A stage is tried at most 4 times.
- **No executor left**: the query fails: "every node is gone: no executor can run
  stage N".
- **A task fails `maxTaskFailures` times** (out of memory, out of disk): the query
  fails with Spark's message, naming the task (stage, partition, attempt) and its
  error, which says what to do.

### Time and the cluster (`sim.ts`)

Executor *i* is on node *i*; `sim.ts` turns the driver's commands into tasks on cores,
and a stopped node into ExecutorLost. A task is a unit on its slot's core: CPU from
the shared cost model (or measured), reading Parquet from storage, writing and reading
shuffle files and spilled runs on the node's disk (the spill write and spill read
segments), and fetching a shuffle file from another node, or a broadcast from the
driver, over the network.

Its kinds of step: `SQLExecutionStart`, `JobStart`, `StageSubmitted`, `TaskStart` (a
LaunchTask), `TaskRan` (an executor ran a task: `Session.next` stops at these),
`TaskEnd` (the driver handles its TaskFinished or TaskFailed), `StageCompleted`,
`JobEnd`, `ExecutorLost`, `FetchFailed`, `StageResubmitted`, `SQLExecutionEnd` and
`SQLExecutionFailed`. Its state has the Spark plan's operators (with the stage each
belongs to and the rows it gave out), the join choices, jobs, stages (attempts, tasks,
bytes), tasks, the shuffles' map outputs, and each executor's slots, shuffle files and
broadcasts.

## API (`api/`)

- **CompiledQuery** (`query.ts`): SQL → logical (analyzer) → optimized (rules, with
  the trace and timings) → an Execution on its engine. `phases` records the real
  time of parse, analyze, optimize and the engine's planning ("physical").
  Options: `rules`, `engine` (by name), `settings` (the engine's own; `planner` is
  another name for them), `cluster` (`nodes`, or `workers`, and the rest of the
  cluster's settings). The optimized plan is immutable: every `start()` plans and
  runs it afresh.
- **Database** (`database.ts`), on any `SqlDb` and `FileStore`, with the engines it is
  given: tables (`listTables`, `describe`, `files`, `history`, `createTable`, `dropTable`,
  `writer`, `append`, `replace`, `revert`, the `file_rows` setting), loaders
  (`inferCsv`, `importCsv`, `defaultGenerators`, `generate`, `loadSample`,
  `loadTpcds`, `loadSsb`, `loadTaxi`) and queries (`compile`, `query` → `{columns, rows}`).
- `openDatabase(folder?)` (`src/node.ts`): the catalog in `node:sqlite`, data files in
  the folder (in memory without one), and every engine.

## Workbench (`ui/`, built into `docs/`)

A single page; the engine runs in a **Web Worker** so loads and queries never block
it. Catalog and data files live in OPFS, so tables survive a reload; where OPFS is
not available (another tab holds it, a private window) everything is in memory.

- **Session** (`session.ts`) answers the page with JSON-friendly values: reference
  data (`info`: storage, engines (with their kinds of step), rules, cluster options,
  types, generators, TPC-DS scales, examples, functions;
  `schema` for completion), tables (`tables`, `table` at any version, create, drop,
  revert, settings), loaders with progress events, and queries: `check(sql)` (parse
  and analyze for the editor), `compile`, `step(from)` (the next step after
  `from`, running the simulation only when that step has not happened yet),
  `next(from)` (the next step where a unit ran, by the engine's `unitSteps`: for
  kostka, the next split processed), `wave`, `finish`, `run`,
  `stopNode(n)` (a failure at the latest step), `state(at)` (the engine's state after
  any step, with the error and the first result rows),
  `close`. Answers carry the state, its step and the steps the
  page does not have yet.
- **RPC** (`rpc.ts`, `worker.ts`): `{id, name, args}` → `{id, ok, value | error}`, with
  progress `{id, event}`; calls run one at a time.
- **SQL editor** (`editor.ts`): CodeMirror 6 with completion of tables, columns
  (with their types, through aliases) and functions (with signatures) from the
  catalog, and squiggles under parse and analysis errors as you type.
- **Tutorials** (`app/tutorial.tsx`): a Tutorial button opens five guided scenarios:
  your first query (NYC taxi), how the work is split (SSB Q2.1: broadcast and shuffle
  joins, splits, partitions), a worker fails (NYC taxi on 3 workers: lost outputs,
  recovery, then every worker stopped), what the optimizer saves you (SSB Q4.1 with
  rules turned off: refused cross joins, then the cost of reading every column), and
  Spark-style: stages, shuffles and slots (the sample on the Spark-style engine:
  "revenue per city" shuffles and "orders from one city" broadcasts, a higher threshold
  broadcasts the first too, 2 cores run tasks side by side and wait for IO, and a node
  stopped in the last stage gives FetchFailed and a resubmitted stage). kostka's
  tutorials run on kostka: starting one sets its query's engine to kostka. Each
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
  with: the **engine** ("kostka (pipelines)" or "Spark-style (stages and tasks)"), then
  five groups (optimizer rules; the engine's own settings: for kostka, join
  distribution, shuffle partitions, runtime filters; for Spark, the broadcast threshold,
  shuffle partitions and max task failures; cluster: nodes, cores per node, memory,
  disk, IO speeds; data flow: batch and split size; simulation: CPU time), Compile /
  Step / Next split (Next task) / Wave / Finish / Run, a
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
  runtime filters), workers (the timeline, time by category, memory,
  disk and cached tables, and a button to stop a worker), steps (the list,
  filtered by kind, each with its commands and events; click one to go to it),
  trace (one Gantt tree on one time axis from the start of parsing: parse,
  analyze, each optimizer pass and physical planning in real time, then the
  simulated execution from where compilation ended, with a line marking the switch;
  execution goes down to pipelines and then workers, each split a bar in its
  pipeline's color, of 20)
  and result. All of them
  show the query as it was right after the step chosen. The timeline has a row per
  node and a lane per core in it; a unit's time waiting for the node's IO channel is
  hatched. Plan, steps, trace, the timeline and result are shared by every engine
  (`app/timeline.tsx`, `events.tsx`, `trace.tsx`); program, query state and workers
  are kostka's own (`app/engines/kostka/`), and the query tab picks an engine's views
  by `state.engine` (`app/engines/index.ts`). The Spark-style engine's own views
  (`app/engines/spark/`) are in Spark's orange, and the query's header says
  "Spark-style engine": **Plan by stage** (in place of Program: the stages as cards in
  columns from the scans to the result, each with its operators in a WholeStageCodegen
  box and its Exchange or BroadcastExchange as the edge to the stages that read it;
  click a join to see why it was broadcast or shuffled), **Jobs · Stages · Tasks** (in
  place of Query state, after Spark's web UI: jobs with their stages done; stages with
  their attempt, tasks done, running and failed, input, shuffle read and write, spill
  and time; the tasks of the stage chosen with partition, attempt, executor, slot,
  status, time, spill and error) and **Executors** (in place of Workers: the timeline,
  each executor's slots, busy or free and with which task, the memory per running
  task, what its node holds, its shuffle files and broadcasts, failed tasks, and Stop).

`npm run dev` rebuilds on every change and serves `docs/` on http://127.0.0.1:8765
(unminified, with source maps). `npm run build` bundles `src/ui/main.tsx` → `docs/app.js` and `src/ui/worker.ts` →
`docs/worker.js` (esbuild, class names kept: plan nodes are named by their class),
and copies `sqlite3.wasm`, the stylesheet and `index.html`.

## Tests (`test/`)

`node --test`, mirroring `src/`. Query results are checked against SQLite
(`node:sqlite`) on the same rows: hand-written queries over small tables (on kostka,
and on the Spark-style engine with 1, 3 and 4 nodes, 1 and 2 cores, and broadcasts off
or always), and every NYC taxi, SSB and TPC-DS example on 1, 4 and 8 workers, ignoring
row order unless the query sorts. The Spark-style engine's tests (`test/engines/spark/`)
cover the planner (broadcast or shuffle, jobs, stages and task counts), the driver and
the executor with no simulation, stages as barriers, slots and cores, recovery from a
node stopped at 20 steps, out of memory after 4 attempts, and a sort that spills; the
shared kernels have their own (`test/core/kernels.test.ts`).
kostka's steps and rows are checked against a fixture recorded before the engine seam
(`test/fixtures/golden-kostka.json`), on 1 and 2 cores. A toy engine registered only
in the tests runs through the Database and the Session, the cluster's parts have
unit tests with no engine, and a test checks that the shared layers import no engine.
Also: functions and casts (including int overflow), the parser and analyzer (errors
and their spans), each optimizer rule, execution (the protocol's order, partition
owners and moves, cutting splits from outputs, self-contained splits, limits,
spilling, out of memory, recovery from a stopped worker at every step of a query,
with the same result), the catalog (commits,
conflicts, time travel, revert), storage (Parquet round trips, statistics, sketches),
the writer (compaction, retries), CSV, NYC taxi loading, SSB and TPC-DS generation, and the Session.
