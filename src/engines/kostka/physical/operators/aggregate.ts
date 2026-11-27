// Two-phase aggregation.
//
// PartialAggregateSink: each split folds its rows into groups (in first-seen
// order), each group holding one state per aggregate. When the split ends, it writes
// every group's key and states to the partition of its output buffer its key hashes
// to (one partition, gathered, for a global aggregate). A group seen by several
// splits is written once by each; the merge adds them up.
//
// MergeAggregateSource: one split per partition, always the whole partition (a group's
// partial states must all meet in one place), on the partition's owner. It merges
// the states into final groups and emits key + result rows in batches. A global
// aggregate over no rows still produces its one row.

import { compileExpr, type Evaluator } from "../../../../core/compile.ts";
import type { AggCall, Expr, Field } from "../../../../core/expr.ts";
import type { BoundAggregate } from "../../../../core/functions/index.ts";
import { partitionOf } from "../../../../core/hash.ts";
import { tupleKey, type Row, type Value } from "../../../../core/types.ts";
import { Batch, checkColumns } from "../batch.ts";
import type { Charged, ExecContext } from "../context.ts";
import { COST } from "../../../../core/cost.ts";
import type { BufferSplit, OutputBuffer } from "../buffer.ts";
import { ENTRY_OVERHEAD, rowsBytes } from "../../../../core/rows.ts";
import { Sink, Source, type OperatorInstance, type SourceInput, type SourceInstance } from "../operator.ts";

interface Group {
  key: Value[];
  states: Value[][];
}

/** The aggregate implementations and where each one's state sits in a partial row. */
export class AggLayout {
  readonly impls: BoundAggregate[];
  readonly offsets: number[];
  readonly width: number;
  constructor(calls: readonly AggCall[]) {
    this.impls = calls.map((c) => c.bound);
    this.offsets = [];
    let o = 0;
    for (const impl of this.impls) {
      this.offsets.push(o);
      o += impl.state.length;
    }
    this.width = o;
  }
  init(): Value[][] {
    return this.impls.map((i) => i.init());
  }
}

export class PartialAggregateSink extends Sink {
  readonly buffer: OutputBuffer;
  readonly calls: readonly AggCall[];
  readonly layout: AggLayout;
  private readonly keys: Evaluator[];
  private readonly args: (Evaluator | null)[];

  constructor(buffer: OutputBuffer, keys: readonly Expr[], calls: readonly AggCall[], columns: readonly Field[]) {
    super(columns);
    this.buffer = buffer;
    this.calls = calls;
    this.layout = new AggLayout(calls);
    this.keys = keys.map((k) => compileExpr(k, columns));
    this.args = calls.map((c) => (c.arg ? compileExpr(c.arg, columns) : null));
  }

  label(): string {
    return "PartialAggregate";
  }

  override writes(): OutputBuffer {
    return this.buffer;
  }

  instance(me: Charged): OperatorInstance {
    const groups = new Map<Value, Group>();
    const { impls } = this.layout;
    const args = this.args;
    let holdKey: string | null = null;
    let perGroup = 0;
    let groupsSent = 0;
    let sample: Value[][] = [];
    return {
      push: (ctx: ExecContext, batch: Batch) => {
        checkColumns(batch, this.input, "PartialAggregate");
        me.metrics.tookIn(batch);
        if (!holdKey) {
          holdKey = `groups:${this.id}:S${ctx.split.id}`;
          ctx.hold(holdKey, "groups", `P${ctx.pipeline} partial groups`, 0, me);
        }
        for (const row of batch.rows) {
          const kv = this.keys.map((k) => k(row));
          const ck = tupleKey(kv);
          let g = groups.get(ck);
          if (!g) {
            g = { key: kv, states: this.layout.init() };
            groups.set(ck, g);
          }
          for (let i = 0; i < impls.length; i++) {
            const a = args[i];
            const v = a ? a(row) : true;
            if (v !== null) impls[i]!.add(g.states[i]!, v);
          }
        }
        ctx.cpu(batch.rows.length * (COST.hashProbe + impls.length * COST.aggUpdate), me);
        if (!perGroup && groups.size) {
          const s = [...groups.values()].slice(0, 16).map((g) => [...g.key, ...g.states.flat()]);
          perGroup = rowsBytes(s) / s.length;
        }
        ctx.grow(holdKey, Math.round(groups.size * (perGroup + ENTRY_OVERHEAD)), me);
        return true;
      },
      finish: (ctx) => {
        const n = this.buffer.n;
        const parts: Row[][] = Array.from({ length: n }, () => []);
        for (const g of groups.values()) parts[partitionOf(g.key, n)]!.push([...g.key, ...g.states.flat()]);
        ctx.cpu(groups.size * COST.partition, me);
        parts.forEach((rows, p) => rows.length && ctx.store(this.buffer, p, rows, me));
        groupsSent += groups.size;
        if (!sample.length) sample = [...groups.values()].slice(0, 20).map((g) => [...g.key, ...g.states.map((st, i) => impls[i]!.result(st))]);
        if (groups.size) ctx.note(me, `${groups.size.toLocaleString("en")} group(s) written to ${parts.filter((x) => x.length).length} partition(s)`);
        groups.clear();
        if (holdKey) ctx.release(holdKey);
        holdKey = null;
      },
      state: () => ({ groups: groups.size, groups_sent: groupsSent, sample }),
    };
  }

  override describe() {
    return { aggregates: this.calls.map((c) => c.display()), buffer: `${this.buffer.name} (${this.buffer.describe()})` };
  }
}

export class MergeAggregateSource extends Source {
  readonly buffer: OutputBuffer;
  readonly nKeys: number;
  readonly layout: AggLayout;

  constructor(buffer: OutputBuffer, nKeys: number, layout: AggLayout, output: readonly Field[]) {
    super(output);
    this.buffer = buffer;
    this.nKeys = nKeys;
    this.layout = layout;
  }

  label(): string {
    return "MergeAggregate";
  }

  override reads(): readonly OutputBuffer[] {
    return [this.buffer];
  }

  reading(): SourceInput {
    return { kind: "buffer", buffer: this.buffer, whole: true, always: this.nKeys === 0 ? [0] : [] };
  }

  instance(me: Charged): SourceInstance {
    let rows: Row[] = [];
    let pos = 0;
    let holdKey: string | null = null;
    const k = this.nKeys;
    const { impls, offsets } = this.layout;
    let groupsOut = 0, partialsMerged = 0;
    const sample: Row[] = [];
    return {
      open: (ctx, split) => {
        const s = split as BufferSplit;
        holdKey = `groups:${this.id}:S${s.id}`;
        ctx.hold(holdKey, "groups", `P${ctx.pipeline} aggregate groups`, 0, me);
        const groups = new Map<Value, Group>();
        let merged = 0, perGroup = 0;
        for (const piece of ctx.pieces()) {
          const part = ctx.fetch(piece, me);
          for (let i = 0; i < part.length; i++) {
            const row = part[i]!;
            const kv = row.slice(0, k);
            const ck = tupleKey(kv);
            let g = groups.get(ck);
            if (!g) {
              g = { key: kv, states: this.layout.init() };
              groups.set(ck, g);
            }
            for (let a = 0; a < impls.length; a++) {
              const o = k + offsets[a]!;
              impls[a]!.merge(g.states[a]!, row.slice(o, o + impls[a]!.state.length));
            }
          }
          merged += part.length;
          if (!perGroup && part.length) perGroup = rowsBytes(part.slice(0, 16)) / Math.min(16, part.length);
          ctx.cpu(part.length * (COST.hashProbe + impls.length * COST.aggUpdate), me);
          ctx.grow(holdKey, Math.round(groups.size * (perGroup + ENTRY_OVERHEAD)), me);
        }
        if (!k && !groups.size && s.partition === 0) groups.set(0, { key: [], states: this.layout.init() });
        rows = [...groups.values()].map((g) => [...g.key, ...g.states.map((st, a) => impls[a]!.result(st))]);
        pos = 0;
        partialsMerged += merged;
        groupsOut += rows.length;
        if (sample.length < 20) sample.push(...rows.slice(0, 20 - sample.length));
        ctx.note(me, `merged ${merged.toLocaleString("en")} partial group(s) into ${rows.length.toLocaleString("en")}`);
      },
      next: (ctx) => {
        if (pos >= rows.length) {
          if (holdKey) ctx.release(holdKey);
          holdKey = null;
          return null;
        }
        const out = rows.slice(pos, pos + ctx.batchRows);
        pos += out.length;
        const b = new Batch(this.output, out);
        me.metrics.gaveOut(b);
        return b;
      },
      state: () => ({ partials_merged: partialsMerged, groups: groupsOut, sample }),
    };
  }

  override describe() {
    return { buffer: this.buffer.name, columns: this.output.map((f) => f.label()) };
  }
}
