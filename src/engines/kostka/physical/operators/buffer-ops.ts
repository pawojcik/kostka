// Writing to and reading from output buffers: BufferSink (shuffle, broadcast,
// gather), BufferSource (outputs of a partition, possibly cut into several splits)
// and ResultSink.

import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import type { Expr, Field } from "../../../../core/expr.ts";
import { partitionOf } from "../../../../core/hash.ts";
import type { Row, Value } from "../../../../core/types.ts";
import { Batch, checkColumns } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST } from "../../../../cluster/cost.ts";
import type { OutputBuffer, PieceRef } from "../buffer.ts";
import { rowsBytes } from "../../../../cluster/rows.ts";
import { Sink, Source, type OperatorInstance, type SourceInput, type SourceInstance } from "../operator.ts";
import type { RuntimeFilter } from "./runtime-filter.ts";

/** Writes rows to its pipeline's output buffer. A split buffers rows per partition
 * and writes a piece when a partition's rows reach the batch size, and the rest when it
 * ends. As the build side of an inner join it also collects the keys for runtime
 * filters, and reports them when its split ends. */
export class BufferSink extends Sink {
  readonly buffer: OutputBuffer;
  private readonly keys: Evaluator[];
  readonly keyExprs: readonly Expr[];
  readonly runtimeFilters: RuntimeFilter[] = [];

  constructor(buffer: OutputBuffer, keys: readonly Expr[], columns: readonly Field[]) {
    super(columns);
    this.buffer = buffer;
    this.keyExprs = keys;
    this.keys = keys.map((k) => compileExpr(k, columns));
  }

  label(): string {
    const b = this.buffer;
    return b.mode === "shuffle" ? `Shuffle(${this.keys.length} key(s), ${b.n} partitions)` : b.mode === "broadcast" ? "Broadcast" : "Gather";
  }

  override writes(): OutputBuffer {
    return this.buffer;
  }

  instance(me: Charged): OperatorInstance {
    const b = this.buffer;
    const parts: Row[][] = Array.from({ length: b.n }, () => []);
    const sent: number[] = new Array(b.n).fill(0);
    const collected = this.runtimeFilters.map(() => new Set<Value>());
    let holdKey: string | null = null;
    let buffered = 0;
    const flush = (ctx: ExecContext, p: number) => {
      const rows = parts[p]!;
      if (!rows.length) return;
      parts[p] = [];
      buffered -= rows.length;
      ctx.store(b, p, rows, me);
      sent[p]! += rows.length;
    };
    const account = (ctx: ExecContext, sample: readonly Row[]) => {
      if (!holdKey) {
        holdKey = `buffer:${this.id}:S${ctx.split.id}`;
        ctx.hold(holdKey, "buffer", `P${ctx.pipeline} rows not written yet`, 0, me);
      }
      ctx.grow(holdKey, Math.round((rowsBytes(sample) / Math.max(1, sample.length)) * buffered), me);
    };
    return {
      push: (ctx, batch: Batch) => {
        checkColumns(batch, this.input, "BufferSink");
        me.metrics.tookIn(batch);
        const rows = batch.rows;
        if (collected.length) {
          for (const row of rows) {
            const vs: Value[] = this.keys.map((k) => k(row));
            if (vs.includes(null)) continue;
            this.runtimeFilters.forEach((rf, i) => collected[i]!.add(vs[rf.position]!));
          }
        }
        if (b.mode === "shuffle" && b.n > 1) {
          ctx.cpu(rows.length * COST.partition, me);
          const keys = this.keys;
          for (const row of rows) parts[partitionOf(keys.map((k) => k(row)), b.n)]!.push(row);
        } else {
          ctx.cpu(rows.length * COST.row, me);
          for (const row of rows) parts[0]!.push(row);
        }
        buffered += rows.length;
        for (let p = 0; p < b.n; p++) if (parts[p]!.length >= ctx.batchRows) flush(ctx, p);
        if (buffered) account(ctx, rows);
        return true;
      },
      finish: (ctx) => {
        for (let p = 0; p < b.n; p++) flush(ctx, p);
        if (holdKey) ctx.release(holdKey);
        this.runtimeFilters.forEach((rf, i) => ctx.collectedKeys(rf.id, collected[i]!));
      },
      state: () => ({
        rows_written: sent.reduce((a, x) => a + x, 0), written_per_partition: [...sent],
        runtime_filter_keys: this.runtimeFilters.map((rf, i) => ({ filter: rf.label, keys: collected[i]!.size })),
      }),
    };
  }

  override describe() {
    return {
      buffer: `${this.buffer.name} (${this.buffer.describe()})`, partitions: this.buffer.n,
      keys: this.keyExprs.map((k) => k.display()), runtime_filters: this.runtimeFilters.map((rf) => rf.label),
    };
  }
}

/** Reads a buffer's partitions: the outputs of a partition, cut into splits of up to
 * `splitRows` (or, `whole`, all of a partition in one split), emitted as batches. */
export class BufferSource extends Source {
  readonly buffer: OutputBuffer;
  readonly whole: boolean;

  constructor(buffer: OutputBuffer, whole = false) {
    super(buffer.columns);
    this.buffer = buffer;
    this.whole = whole;
  }

  label(): string {
    return { shuffle: "ShuffleRead", broadcast: "BroadcastRead", gather: "GatherRead" }[this.buffer.mode];
  }

  override reads(): readonly OutputBuffer[] {
    return [this.buffer];
  }

  reading(): SourceInput {
    return { kind: "buffer", buffer: this.buffer, whole: this.whole, always: [] };
  }

  instance(me: Charged): SourceInstance {
    let pieces = 0, rows = 0;
    const reader = batchReader(this, me, (n) => {
      pieces += n.pieces;
      rows += n.rows;
    });
    return { ...reader, state: () => ({ pieces_read: pieces, rows_received: rows }) };
  }

  override describe() {
    return { buffer: `${this.buffer.name} (${this.buffer.describe()})`, whole: this.whole };
  }
}

/** Reads a split's pieces in order as batches, fetching each piece when needed. */
export function batchReader(op: Source, me: Charged, count: (n: { pieces: number; rows: number }) => void): SourceInstance {
  let pieces: readonly PieceRef[] = [];
  let i = 0;
  let cur: readonly Row[] = [];
  let pos = 0, end = 0;
  return {
    open: (ctx) => {
      pieces = ctx.pieces();
      i = 0;
      cur = [];
      pos = end = 0;
    },
    next: (ctx) => {
      const out: Row[] = [];
      const m = ctx.batchRows;
      while (out.length < m) {
        if (pos < end) {
          const take = Math.min(end - pos, m - out.length);
          for (let k = 0; k < take; k++) out.push(cur[pos + k]!);
          pos += take;
          continue;
        }
        const p = pieces[i++];
        if (!p) break;
        cur = ctx.fetch(p, me);
        pos = 0;
        end = p.rows;
        count({ pieces: 1, rows: p.rows });
        const where = p.worker === ctx.worker ? "local" : `from W${p.worker}`;
        ctx.note(me, `read the piece of split ${p.split} (${p.rows.toLocaleString("en")} rows, ${where})`);
      }
      if (!out.length) return null;
      ctx.cpu(out.length * COST.exchangeRow, me);
      const b = new Batch(op.output, out);
      me.metrics.gaveOut(b);
      return b;
    },
  };
}

/** Sends the split's rows to the coordinator when it ends (with SplitDone). The
 * result's row order is the order splits finished in: only a Sort below it fixes the
 * order. */
export class ResultSink extends Sink {
  constructor(columns: readonly Field[]) {
    super(columns);
  }
  label(): string {
    return "Result";
  }
  instance(me: Charged): OperatorInstance {
    let buf: Row[] = [];
    let sent = 0;
    const send = (ctx: ExecContext) => {
      if (!buf.length) return;
      ctx.send(buf, me);
      sent += buf.length;
      buf = [];
    };
    return {
      push: (_ctx, batch) => {
        checkColumns(batch, this.input, "Result");
        me.metrics.tookIn(batch);
        for (const r of batch.rows) buf.push(r);
        return true;
      },
      finish: send,
      state: () => ({ rows_sent: sent }),
    };
  }
}
