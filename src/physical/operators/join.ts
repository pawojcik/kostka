// The hash join. The build side (the join's right input) ends in an output buffer
// that every probe split reads whole (the coordinator sends its pieces with the
// split); the probe side (left) streams through HashProbeOp, which looks up the
// hash table in the worker's cache and builds it from those pieces if it is not
// there yet:
//
// - broadcast: one table per worker, from every piece.
// - shuffle: one table per partition, on the partition's owner (where all its
//   splits go).
//
// The cache keeps a table until the coordinator releases the build buffer. It is
// read-only and can always be built again, so it is not state the split depends on.
//
// Null keys never match. Output: each probe row followed by its matches, in build
// order; a left join pads unmatched probe rows with nulls. `residual` is the rest of
// the join condition, evaluated on each candidate pair.

import { compileExpr, compilePredicate, type Evaluator } from "../../core/compile.ts";
import type { Expr, Field } from "../../core/expr.ts";
import { tupleKey, type Row, type Value } from "../../core/types.ts";
import type { JoinType } from "../../sql/plan.ts";
import { checkColumns } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST } from "../cost.ts";
import type { OutputBuffer } from "../buffer.ts";
import { ENTRY_OVERHEAD, perRowBytes } from "../metrics.ts";
import { Operator, emit, type OperatorInstance, type WholeInput } from "../operator.ts";

export class HashTable {
  readonly buckets = new Map<Value, Row[]>();
  rows = 0;
  nullKeys = 0;
  bytes = 0;
  private perRow = 0;

  add(rows: readonly Row[], keys: readonly Evaluator[]): void {
    for (const row of rows) {
      const k = keyOf(row, keys);
      if (k === undefined) {
        this.nullKeys++;
        continue;
      }
      const b = this.buckets.get(k);
      if (b) b.push(row);
      else this.buckets.set(k, [row]);
      this.rows++;
    }
    if (rows.length && !this.perRow) this.perRow = perRowBytes(rows);
    this.bytes = Math.round(this.rows * this.perRow + this.buckets.size * ENTRY_OVERHEAD);
  }
}

/** The key of a row, or undefined if any key value is null (it never matches). */
export function keyOf(row: Row, keys: readonly Evaluator[]): Value | undefined {
  if (keys.length === 0) return 0; // no equality keys: one bucket
  if (keys.length === 1) {
    const v = keys[0]!(row);
    return v === null ? undefined : v;
  }
  const vs: Value[] = [];
  for (const k of keys) {
    const v = k(row);
    if (v === null) return undefined;
    vs.push(v);
  }
  return tupleKey(vs);
}

export interface TableInfo {
  where: string;
  rows: number;
  keys: number;
  bytes: number;
  nullKeys: number;
  builds: number;
}

export class HashProbeOp extends Operator {
  readonly build: OutputBuffer;
  readonly buildColumns: readonly Field[];
  readonly how: JoinType;
  private readonly buildKeys: Evaluator[];
  private readonly probeKeys: Evaluator[];
  private readonly residual: ((row: Row) => boolean) | null;
  readonly residualExpr: Expr | null;
  readonly keyCount: number;

  constructor(build: OutputBuffer, buildKeys: readonly Expr[], probeKeys: readonly Expr[], residual: Expr | null, how: JoinType,
    left: readonly Field[], right: readonly Field[]) {
    super(left, [...left, ...right]);
    this.build = build;
    this.buildColumns = right;
    this.how = how;
    this.buildKeys = buildKeys.map((k) => compileExpr(k, right));
    this.probeKeys = probeKeys.map((k) => compileExpr(k, left));
    this.residualExpr = residual;
    this.residual = residual ? compilePredicate(residual, this.output) : null;
    this.keyCount = buildKeys.length;
  }

  label(): string {
    return `HashProbe(${this.how}, ${this.keyCount} key(s))`;
  }

  override reads(): readonly OutputBuffer[] {
    return [this.build];
  }

  override wholeInputs(): readonly WholeInput[] {
    return [{ buffer: this.build, per: this.build.mode === "broadcast" ? "all" : "partition" }];
  }

  instance(me: Charged, next: OperatorInstance): OperatorInstance {
    let table: HashTable | null = null;
    let info: TableInfo | null = null;
    const broadcast = this.build.mode === "broadcast";
    const pad: Row = new Array(this.buildColumns.length).fill(null);
    let probes = 0, hits = 0, candidatesAll = 0, padded = 0;
    let sample: { key: Value; rows: Row[]; count: number }[] = [];

    const buildTable = (ctx: ExecContext, partition: number, holdKey: string): { t: HashTable; info: TableInfo } => {
      const t = new HashTable();
      ctx.hold(holdKey, "hash", `P${ctx.pipeline} hash table${broadcast ? "" : ` p${partition}`} (cached)`, 0, me,
        broadcast ? " (with a broadcast join every worker holds the whole build side; a shuffle join holds one partition)." : undefined);
      for (const piece of ctx.input(this.build.index, partition)) {
        const rows = ctx.fetch(piece, me);
        ctx.cpu(rows.length * COST.hashInsert, me);
        t.add(rows, this.buildKeys);
        ctx.grow(holdKey, t.bytes, me);
      }
      const where = broadcast ? "broadcast" : `partition #${partition}`;
      const i: TableInfo = { where, rows: t.rows, keys: t.buckets.size, bytes: t.bytes, nullKeys: t.nullKeys, builds: 1 };
      ctx.note(me, `built the ${where} hash table: ${t.rows.toLocaleString("en")} rows, ${t.buckets.size.toLocaleString("en")} keys, ${t.bytes.toLocaleString("en")} bytes`);
      ctx.tableBuilt(me, { partition: broadcast ? null : partition, rows: t.rows, keys: t.buckets.size, bytes: t.bytes, again: false },
        `W${ctx.worker} built the ${where} hash table of ${this.id} and cached it: ${t.rows.toLocaleString("en")} rows, ${t.buckets.size.toLocaleString("en")} keys, ${t.bytes.toLocaleString("en")} bytes`);
      return { t, info: i };
    };

    const get = (ctx: ExecContext): HashTable => {
      if (table) return table;
      const partition = broadcast ? 0 : (ctx.split.partition ?? 0);
      const key = `hash:${this.id}:p${partition}`;
      const c = ctx.cache(this.build.index, key, (holdKey) => buildTable(ctx, partition, holdKey));
      if (c.hit) ctx.note(me, `reused the cached ${c.value.info.where} hash table (${c.value.t.rows.toLocaleString("en")} rows)`);
      info = { ...c.value.info, builds: c.hit ? 0 : 1 };
      if (!sample.length) sample = [...c.value.t.buckets].slice(0, 12).map(([k, rows]) => ({ key: k, rows: rows.slice(0, 5), count: rows.length }));
      table = c.value.t;
      return table;
    };

    return {
      push: (ctx, batch) => {
        checkColumns(batch, this.input, "HashProbe");
        me.metrics.tookIn(batch);
        const buckets = get(ctx).buckets;
        // the output goes on in batches of at most batchRows: a join that multiplies
        // rows (a cross join, a skewed key) never builds one huge array
        let out: Row[] = [];
        let candidates = 0, probed = 0;
        const flush = (): boolean => {
          ctx.cpu(probed * COST.hashProbe + out.length * COST.joinOutput, me);
          probed = 0;
          const rows = out;
          out = [];
          return emit(me, this, ctx, next, rows);
        };
        for (const lrow of batch.rows) {
          probes++;
          probed++;
          const k = keyOf(lrow, this.probeKeys);
          const bucket = k === undefined ? undefined : buckets.get(k);
          let matched = false;
          if (bucket) {
            hits++;
            for (const rrow of bucket) {
              candidates++;
              const row = lrow.concat(rrow);
              if (!this.residual || this.residual(row)) {
                out.push(row);
                matched = true;
                if (out.length >= ctx.batchRows && !flush()) {
                  candidatesAll += candidates;
                  return false;
                }
              }
            }
          }
          if (!matched && this.how === "left") {
            out.push(lrow.concat(pad));
            padded++;
          }
        }
        candidatesAll += candidates;
        return flush();
      },
      finish: (ctx) => {
        table = null;
        next.finish(ctx);
      },
      state: () => ({
        probes, probes_with_bucket: hits, candidate_pairs: candidatesAll, padded_rows: padded,
        tables: info ? [info] : [], sample,
      }),
    };
  }

  override describe() {
    return {
      how: this.how, keys: this.keyCount, build: `${this.build.name} (${this.build.mode})`, residual: this.residualExpr?.display() ?? null,
      build_columns: this.buildColumns.map((f) => f.label()),
    };
  }
}
