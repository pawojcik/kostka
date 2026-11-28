// Sort and top-n, in two halves.
//
// SortSink: each split buffers its rows and, when it ends, sorts them into one run
// (a piece of a gather buffer). TopNSink keeps only the split's best n rows in a
// bounded heap instead.
// MergeSortSource: reads every run as one split (the gather's whole partition) and
// merges them k-way, stopping after n rows for a top-n.
//
// Rows with equal keys may come out in any order: only the sort keys are ordered.

import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import type { Field, SortKey } from "../../../../core/expr.ts";
import type { Row, Value } from "../../../../core/types.ts";
import { TopK, bestRun, keyComparator, type Keyed } from "../../../../core/row-order.ts";
import { Batch, checkColumns } from "../../../../core/batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST, sortCost } from "../../../../core/cost.ts";
import type { OutputBuffer } from "../buffer.ts";
import { rowsBytes } from "../../../../core/rows.ts";
import { Sink, Source, type OperatorInstance, type SourceInput, type SourceInstance } from "../operator.ts";

abstract class SortingSink extends Sink {
  readonly buffer: OutputBuffer;
  readonly keys: readonly SortKey[];
  protected readonly evals: Evaluator[];
  protected readonly cmp: (a: Value[], b: Value[]) => number;
  constructor(buffer: OutputBuffer, keys: readonly SortKey[], columns: readonly Field[]) {
    super(columns);
    this.buffer = buffer;
    this.keys = keys;
    this.evals = keys.map((k) => compileExpr(k.expr, columns));
    this.cmp = keyComparator(keys);
  }
  override writes(): OutputBuffer {
    return this.buffer;
  }
  protected keyed(row: Row): Keyed {
    return { k: this.evals.map((e) => e(row)), row };
  }
  /** Writes a split's sorted run; returns its first rows, for the workbench. */
  protected writeRun(ctx: ExecContext, me: Charged, items: Keyed[]): Row[] {
    if (!items.length) return [];
    const rows = items.map((x) => x.row);
    ctx.store(this.buffer, 0, rows, me);
    ctx.note(me, `wrote a sorted run of ${rows.length.toLocaleString("en")} rows`);
    return rows.slice(0, 20);
  }
  override describe() {
    return { keys: this.keys.map((k) => k.toString()), buffer: this.buffer.name };
  }
}

export class SortSink extends SortingSink {
  label(): string {
    return "SortSink";
  }
  instance(me: Charged): OperatorInstance {
    let buf: Keyed[] = [];
    let holdKey: string | null = null;
    let sample: Row[] = [];
    return {
      push: (ctx, batch: Batch) => {
        checkColumns(batch, this.input, "SortSink");
        me.metrics.tookIn(batch);
        if (!holdKey) {
          holdKey = `sort:${this.id}:S${ctx.split.id}`;
          ctx.hold(holdKey, "sort", `P${ctx.pipeline} sort buffer`, 0, me);
        }
        for (const row of batch.rows) buf.push(this.keyed(row));
        ctx.cpu(batch.rows.length * (COST.row + this.evals.length * COST.exprNode), me);
        ctx.grow(holdKey, Math.round((rowsBytes(batch.rows) / Math.max(1, batch.rows.length)) * buf.length), me);
        return true;
      },
      finish: (ctx) => {
        ctx.cpu(sortCost(buf.length), me);
        buf.sort((a, b) => this.cmp(a.k, b.k));
        sample = this.writeRun(ctx, me, buf);
        buf = [];
        if (holdKey) ctx.release(holdKey);
        holdKey = null;
      },
      state: () => ({ buffered: buf.length, sample }),
    };
  }
}

/** Keeps the best n rows of a split's input in a heap whose root is the worst kept. */
export class TopNSink extends SortingSink {
  readonly n: number;
  constructor(buffer: OutputBuffer, keys: readonly SortKey[], n: number, columns: readonly Field[]) {
    super(buffer, keys, columns);
    this.n = n;
  }
  label(): string {
    return `TopNSink(${this.n})`;
  }
  instance(me: Charged): OperatorInstance {
    const heap = new TopK(this.n, this.cmp);
    let sample: Row[] = [];
    return {
      push: (ctx, batch) => {
        checkColumns(batch, this.input, "TopNSink");
        me.metrics.tookIn(batch);
        for (const row of batch.rows) heap.offer(this.keyed(row));
        ctx.cpu(batch.rows.length * (COST.row + this.evals.length * COST.exprNode + Math.log2(this.n + 1) * COST.sortCompare), me);
        return true;
      },
      finish: (ctx) => {
        sample = this.writeRun(ctx, me, heap.take());
      },
      state: () => ({ seen: heap.seen, replaced: heap.replaced, rejected: heap.rejected, kept: heap.size, sample }),
    };
  }
  override describe() {
    return { ...super.describe(), k: this.n };
  }
}

export class MergeSortSource extends Source {
  readonly buffer: OutputBuffer;
  readonly keys: readonly SortKey[];
  readonly limit: number | null;
  private readonly evals: Evaluator[];
  private readonly cmp: (a: Value[], b: Value[]) => number;

  constructor(buffer: OutputBuffer, keys: readonly SortKey[], limit: number | null) {
    super(buffer.columns);
    this.buffer = buffer;
    this.keys = keys;
    this.limit = limit;
    this.evals = keys.map((k) => compileExpr(k.expr, buffer.columns));
    this.cmp = keyComparator(keys);
  }

  label(): string {
    return this.limit === null ? "MergeSort" : `MergeTopN(${this.limit})`;
  }

  override reads(): readonly OutputBuffer[] {
    return [this.buffer];
  }

  reading(): SourceInput {
    return { kind: "buffer", buffer: this.buffer, whole: true, always: [] };
  }

  instance(me: Charged): SourceInstance {
    let runs: Keyed[][] = [];
    let heads: number[] = [];
    let left = 0;
    let emitted = 0, runCount = 0;
    return {
      open: (ctx) => {
        runs = ctx.pieces().map((p) => ctx.fetch(p, me).map((row) => ({ k: this.evals.map((e) => e(row)), row })));
        heads = runs.map(() => 0);
        left = this.limit ?? Infinity;
        runCount = runs.length;
        ctx.note(me, `merging ${runs.length} sorted run(s), ${runs.reduce((s, r) => s + r.length, 0).toLocaleString("en")} rows`);
      },
      next: (ctx) => {
        const out: Row[] = [];
        while (out.length < ctx.batchRows && left > 0) {
          const best = bestRun(runs, heads, this.cmp);
          if (best < 0) break;
          out.push(runs[best]![heads[best]!++]!.row);
          left--;
        }
        if (!out.length) return null;
        ctx.cpu(out.length * Math.max(1, Math.log2(runs.length + 1)) * COST.sortCompare, me);
        emitted += out.length;
        const b = new Batch(this.output, out);
        me.metrics.gaveOut(b);
        return b;
      },
      state: () => ({ runs: runCount, rows_emitted: emitted }),
    };
  }

  override describe() {
    return { keys: this.keys.map((k) => k.toString()), limit: this.limit, buffer: this.buffer.name };
  }
}
