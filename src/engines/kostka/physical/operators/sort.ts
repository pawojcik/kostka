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
import { compareValues, type Row, type Value } from "../../../../core/types.ts";
import { Batch, checkColumns } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST } from "../../../../core/cost.ts";
import type { OutputBuffer } from "../buffer.ts";
import { rowsBytes } from "../../../../core/rows.ts";
import { Sink, Source, type OperatorInstance, type SourceInput, type SourceInstance } from "../operator.ts";

type Keyed = { k: Value[]; row: Row };

/** A comparator over evaluated keys, honouring ASC/DESC and NULLS FIRST/LAST. */
export function keyComparator(keys: readonly SortKey[]): (a: Value[], b: Value[]) => number {
  return (a, b) => {
    for (let i = 0; i < keys.length; i++) {
      const x = a[i]!, y = b[i]!;
      if (x === y) continue;
      const key = keys[i]!;
      if (x === null || y === null) return (x === null) === key.nullsFirst ? -1 : 1;
      const c = compareValues(x, y);
      if (c) return key.ascending ? c : -c;
    }
    return 0;
  };
}

const sortCost = (n: number) => (n > 1 ? n * Math.log2(n) * COST.sortCompare : 0);

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
    const heap: Keyed[] = []; // a max-heap by sort order: heap[0] sorts last
    let seen = 0, replaced = 0, rejected = 0;
    let sample: Row[] = [];
    const worse = (a: Keyed, b: Keyed) => this.cmp(a.k, b.k) > 0;
    const up = (i: number) => {
      while (i > 0) {
        const p = (i - 1) >> 1;
        if (!worse(heap[i]!, heap[p]!)) break;
        [heap[i], heap[p]] = [heap[p]!, heap[i]!];
        i = p;
      }
    };
    const down = (i: number) => {
      for (;;) {
        const l = 2 * i + 1, r = l + 1;
        let m = i;
        if (l < heap.length && worse(heap[l]!, heap[m]!)) m = l;
        if (r < heap.length && worse(heap[r]!, heap[m]!)) m = r;
        if (m === i) return;
        [heap[i], heap[m]] = [heap[m]!, heap[i]!];
        i = m;
      }
    };
    return {
      push: (ctx, batch) => {
        checkColumns(batch, this.input, "TopNSink");
        me.metrics.tookIn(batch);
        for (const row of batch.rows) {
          seen++;
          const item = this.keyed(row);
          if (heap.length < this.n) {
            heap.push(item);
            up(heap.length - 1);
          } else if (this.n && this.cmp(item.k, heap[0]!.k) < 0) {
            heap[0] = item;
            down(0);
            replaced++;
          } else rejected++;
        }
        ctx.cpu(batch.rows.length * (COST.row + this.evals.length * COST.exprNode + Math.log2(this.n + 1) * COST.sortCompare), me);
        return true;
      },
      finish: (ctx) => {
        const best = [...heap].sort((a, b) => this.cmp(a.k, b.k));
        sample = this.writeRun(ctx, me, best);
        heap.length = 0;
      },
      state: () => ({ seen, replaced, rejected, kept: heap.length, sample }),
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
          let best = -1;
          for (let i = 0; i < runs.length; i++) {
            if (heads[i]! >= runs[i]!.length) continue;
            if (best < 0 || this.cmp(runs[i]![heads[i]!]!.k, runs[best]![heads[best]!]!.k) < 0) best = i;
          }
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
