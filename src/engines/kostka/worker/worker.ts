// A worker: the domain object that runs splits. It has the whole plan (any pipeline
// can run on it), a queue of splits per pipeline, what it holds of the output
// buffers, a cache of hash tables, and its memory. It acts only on commands from the
// coordinator and reports back with events; it never sees the coordinator's state,
// and reads other workers' pieces only through the data plane.
//
//   AddSplits     -> queues them (at most k per pipeline: the coordinator keeps the rest)
//   a split runs  -> SplitDone: the pieces it wrote, runtime filter keys, result rows
//                    (or SplitFailed: an error, or a piece it read is gone)
//   ReleaseBuffer -> frees its pieces of a buffer and the tables built from it
//
// A split is self-contained: its operator instances are made for it and dropped when
// it ends, so the worker keeps nothing of it but the pieces it wrote (and cached
// tables). It runs one split at a time, the one that arrived first. Time, IO cost and
// how a split is timed are infrastructure: its context comes from the WorkerPorts.
//
// The worker lives on a node, whose memory and block store it uses (NodeResources).
// The node's memory decides what spills; the worker says what it holds, and keeps
// the rows of its pieces as the node's blocks.

import type { Row, Value } from "../../../core/types.ts";
import type { OutputBuffer, PieceRef } from "../physical/buffer.ts";
import type { Category, Charged, ExecContext, HoldKind } from "../physical/context.ts";
import { Metrics } from "../physical/metrics.ts";
import type { OperatorInstance, SourceInstance } from "../physical/operator.ts";
import { summarize, type FilterKeys } from "../physical/operators/runtime-filter.ts";
import type { PhysicalPlan, Pipeline } from "../physical/pipeline.ts";
import { PieceLostError, type Assigned, type Command, type OpReport, type UnitReport, type WorkerEvent } from "../protocol.ts";
import { LocalBuffers, holdKeyOf, type LocalPiece } from "./buffers.ts";

/** The rows of a piece, kept on the worker's node. */
export interface PieceBlock {
  readonly id: number;
  readonly node: number;
  readonly rows: readonly Row[];
  readonly bytes: number;
  /** Set by the node's memory when it moves the block to disk. */
  spilled: boolean;
}

/** The unit running when memory is reserved: it pays for what is spilled. */
export interface SpillSink {
  /** `bytes` were written to the local disk to make room (or a block went there). */
  spillWrite(bytes: number): void;
}

/** The node's memory, as a worker uses it. */
export interface NodeMemory {
  /** Reserves memory for a block (which can spill) or for state (no block), spilling
   * blocks to make room; throws OutOfMemoryError, ending with `advice`, if state does
   * not fit. */
  hold(sink: SpillSink, key: string, kind: HoldKind, label: string, bytes: number, block: PieceBlock | null, advice: string): void;
  grow(sink: SpillSink, key: string, bytes: number): void;
  release(key: string): void;
}

/** The node's block store, as a worker uses it. */
export interface NodeBlocks {
  put(rows: readonly Row[]): PieceBlock;
  drop(id: number): void;
}

/** What a worker uses of the node it is on. */
export interface NodeResources {
  readonly memory: NodeMemory;
  readonly blocks: NodeBlocks;
}

/** What to do when state doesn't fit: the end of OutOfMemoryError's message. */
const advice = (hint?: string) => `Give the workers more memory${hint ?? ", or use more shuffle partitions so that each holds less."}`;

/** One operator of one split. */
export class OpRun implements Charged {
  readonly id: string;
  readonly metrics = new Metrics();
  constructor(id: string) {
    this.id = id;
  }
}

/** What a split's context asks of the worker (the domain side of ExecContext). */
export interface SplitHooks {
  store(buffer: OutputBuffer, partition: number, rows: Row[], run: UnitContext): PieceRef;
  send(rows: readonly Row[]): void;
  pieces(): readonly PieceRef[];
  input(buffer: number, partition: number): readonly PieceRef[];
  filterKeys(filter: number): ReadonlySet<Value> | null;
  collectedKeys(filter: number, keys: Iterable<Value>): void;
  cache<T>(buffer: number, key: string, build: (holdKey: string) => T): { value: T; hit: boolean };
  tableBuilt(text: string): void;
  hold(run: UnitContext, key: string, kind: HoldKind, label: string, bytes: number, hint?: string): void;
  grow(run: UnitContext, key: string, bytes: number): void;
  release(key: string): void;
}

/** The context a split runs in: an ExecContext whose time the infrastructure keeps. */
export interface UnitContext extends ExecContext, SpillSink {
  /** Now, in ns, on the worker's timeline. */
  readonly cursor: number;
  /** Ends the split, `finished` or failed: where its time went, and the notes per
   * operator. */
  end(finished: boolean): { start: number; end: number; time: Record<Category, number>; segments: UnitReport["segments"]; notes: Map<Charged, string[]> };
}

/** Infrastructure a worker needs: the context its splits run in. */
export interface WorkerPorts {
  begin(worker: WorkerNode, work: Work, hooks: SplitHooks, start: number): UnitContext;
  /** Wraps an instance so its calls are timed (measured CPU), or returns it as is. */
  instrument(me: Charged, inst: OperatorInstance): OperatorInstance;
  instrumentSource(me: Charged, inst: SourceInstance): SourceInstance;
}

/** A split's operator instances, made when it is about to run. */
interface Chain {
  readonly ops: OpRun[];
  readonly source: SourceInstance;
  readonly instances: OperatorInstance[];
}

/** A queued split. */
export interface Work {
  readonly pipeline: Pipeline;
  readonly assigned: Assigned;
  /** When it arrived. */
  readonly at: number;
  chain?: Chain;
}

interface Cached {
  readonly buffer: number;
  readonly key: string;
  readonly value: unknown;
  readonly holdKey: string;
  hits: number;
}

export class WorkerNode {
  readonly id: number;
  readonly plan: PhysicalPlan;
  readonly node: NodeResources;
  readonly buffers: LocalBuffers;
  /** Pipeline -> its queued splits, in the order they arrived. */
  readonly queues = new Map<number, Work[]>();
  /** Pipeline -> runtime filter keys. */
  readonly filters = new Map<number, Map<number, ReadonlySet<Value>>>();
  readonly cache = new Map<string, Cached>();
  /** Stopped by the infrastructure: it runs nothing more. */
  gone = false;
  /** Events not sent yet, with when they happened. */
  readonly outbox: { at: number; event: WorkerEvent }[] = [];
  private readonly ports: WorkerPorts;

  constructor(id: number, plan: PhysicalPlan, ports: WorkerPorts, node: NodeResources) {
    this.id = id;
    this.plan = plan;
    this.node = node;
    this.buffers = new LocalBuffers(id);
    this.ports = ports;
  }

  private emit(at: number, event: WorkerEvent): void {
    this.outbox.push({ at, event });
  }

  // --- commands -------------------------------------------------------------------

  handle(cmd: Command, now: number): void {
    if (this.gone) return;
    switch (cmd.type) {
      case "AddSplits": {
        const p = this.plan.pipelines[cmd.pipeline]!;
        const q = this.queues.get(p.index) ?? [];
        for (const a of cmd.splits) q.push({ pipeline: p, assigned: a, at: now });
        this.queues.set(p.index, q);
        if (cmd.filters.length) {
          const f = this.filters.get(p.index) ?? new Map<number, ReadonlySet<Value>>();
          for (const k of cmd.filters) f.set(k.filter, new Set(k.keys));
          this.filters.set(p.index, f);
        }
        return;
      }
      case "ReleaseBuffer": {
        for (const piece of this.buffers.release(cmd.buffer)) {
          this.node.memory.release(holdKeyOf(piece.ref));
          this.node.blocks.drop(piece.block.id);
        }
        for (const [k, c] of this.cache) {
          if (c.buffer !== cmd.buffer) continue;
          this.node.memory.release(c.holdKey);
          this.cache.delete(k);
        }
        return;
      }
    }
  }

  // --- running splits -------------------------------------------------------------

  /** The split it would run next: the one that arrived first. */
  next(): Work | null {
    if (this.gone) return null;
    let best: Work | null = null;
    for (const q of this.queues.values()) if (q[0] && (!best || q[0].at < best.at)) best = q[0];
    return best;
  }

  /** Makes the split's operator instances and loads what it needs (a data file). */
  async prepare(work: Work): Promise<void> {
    work.chain ??= this.chain(work.pipeline);
    await work.chain.source.prepare?.(work.assigned.split);
  }

  private chain(p: Pipeline): Chain {
    const ops = p.chain.map((op) => new OpRun(op.id));
    const n = p.chain.length;
    // Each instance gets the instance after it.
    let next = this.ports.instrument(ops[n - 1]!, p.sink.instance(ops[n - 1]!));
    const instances: OperatorInstance[] = [next];
    for (let i = p.operators.length - 1; i >= 0; i--) {
      const me = ops[i + 1]!;
      next = this.ports.instrument(me, p.operators[i]!.instance(me, next));
      instances.unshift(next);
    }
    return { ops, source: this.ports.instrumentSource(ops[0]!, p.source.instance(ops[0]!)), instances };
  }

  /** Runs a split starting at `start`; returns when it ended, and the error if it
   * failed for good (a lost piece is not one: the split can run again). */
  run(work: Work, start: number): { end: number; error: Error | null } {
    const q = this.queues.get(work.pipeline.index)!;
    q.splice(q.indexOf(work), 1);
    const chain = work.chain ?? this.chain(work.pipeline);
    const a = work.assigned;
    const s = a.split;
    const p = work.pipeline;
    const unit = new SplitRun(this, work);
    const run = this.ports.begin(this, work, unit.hooks(), start);
    try {
      chain.source.open(run, s);
      const head = chain.instances[0]!;
      for (;;) {
        const batch = chain.source.next(run);
        if (!batch) break;
        if (!head.push(run, batch)) break;
      }
      head.finish(run);
    } catch (e) {
      unit.abort();
      const err = e as Error;
      const lost = err instanceof PieceLostError ? { worker: err.worker, piece: err.piece } : null;
      this.emit(run.cursor, { type: "SplitFailed", worker: this.id, pipeline: p.index, split: s.id, attempt: a.attempt, error: `${err.name}: ${err.message}`, name: err.name, lost });
      run.end(false);
      return { end: run.cursor, error: lost ? null : err };
    }
    unit.close();
    const report = this.report(chain, run, unit.tables);
    const filters: FilterKeys[] = [...unit.collected].map(([id, keys]) => summarize(id, keys));
    this.emit(report.end, {
      type: "SplitDone", worker: this.id, pipeline: p.index, split: s.id, attempt: a.attempt,
      pieces: unit.written.map((x) => x.ref), filters, result: p === this.plan.root ? unit.result : null, unit: report,
    });
    return { end: report.end, error: null };
  }

  private report(chain: Chain, run: UnitContext, tables: string[]): UnitReport {
    const r = run.end(true);
    const states = [chain.source.state?.() ?? {}, ...chain.instances.map((i) => i.state?.() ?? {})];
    const ops: OpReport[] = chain.ops.map((o, i) => {
      const d = o.metrics;
      return {
        rowsIn: d.rowsIn, rowsOut: d.rowsOut, batchesIn: d.batchesIn, batchesOut: d.batchesOut,
        bytesIn: d.bytesIn, bytesOut: d.bytesOut, time: { ...d.time }, notes: r.notes.get(o) ?? [], state: states[i]!,
      };
    });
    return { start: r.start, end: r.end, time: r.time, segments: r.segments, ops, tables };
  }

  /** What the worker holds, for the workbench: queues, cache and pieces. */
  state(): { worker: number; queued: { pipeline: number; splits: { id: number; attempt: number }[] }[]; cache: { key: string; buffer: number; hits: number }[]; pieces: ReturnType<LocalBuffers["counts"]> } {
    if (this.gone) return { worker: this.id, queued: [], cache: [], pieces: this.buffers.counts() };
    return {
      worker: this.id,
      queued: [...this.queues].filter(([, q]) => q.length).map(([pipeline, q]) => ({ pipeline, splits: q.map((w) => ({ id: w.assigned.split.id, attempt: w.assigned.attempt })) })),
      cache: [...this.cache.values()].map((c) => ({ key: c.key, buffer: c.buffer, hits: c.hits })),
      pieces: this.buffers.counts(),
    };
  }
}

/** What one split did on its worker while it ran: the pieces it wrote, the memory it
 * held, keys and rows it collected. Kept or undone when it ends. */
class SplitRun {
  readonly written: LocalPiece[] = [];
  readonly holds = new Set<string>();
  readonly collected = new Map<number, Set<Value>>();
  readonly result: Row[] = [];
  readonly tables: string[] = [];
  private readonly w: WorkerNode;
  private readonly work: Work;

  constructor(w: WorkerNode, work: Work) {
    this.w = w;
    this.work = work;
  }

  hooks(): SplitHooks {
    const w = this.w, u = this, a = this.work.assigned, p = this.work.pipeline;
    return {
      store(buffer, partition, rows, run) {
        const piece = w.buffers.add(buffer.index, partition, a.split.id, w.node.blocks.put(rows));
        u.written.push(piece);
        w.node.memory.hold(run, holdKeyOf(piece.ref), "output", `P${buffer.producer} output ${buffer.name}`, piece.ref.bytes, piece.block, advice());
        return piece.ref;
      },
      send(rows) {
        for (const r of rows) u.result.push(r);
      },
      pieces: () => a.pieces,
      input: (buffer, partition) => a.inputs.find((i) => i.buffer === buffer && i.partition === partition)?.pieces ?? [],
      filterKeys: (filter) => w.filters.get(p.index)?.get(filter) ?? null,
      collectedKeys(filter, keys) {
        const set = u.collected.get(filter) ?? new Set<Value>();
        for (const k of keys) set.add(k);
        u.collected.set(filter, set);
      },
      cache<T>(buffer: number, key: string, build: (holdKey: string) => T) {
        const c = w.cache.get(key);
        if (c) {
          c.hits++;
          return { value: c.value as T, hit: true };
        }
        const holdKey = `cache:${key}`;
        const value = build(holdKey);
        // the table outlives the split: its memory is the cache's now
        u.holds.delete(holdKey);
        w.cache.set(key, { buffer, key, value, holdKey, hits: 0 });
        return { value, hit: false };
      },
      tableBuilt(text) {
        u.tables.push(text);
      },
      hold(run, key, kind, label, bytes, hint) {
        w.node.memory.hold(run, key, kind, label, bytes, null, advice(hint));
        u.holds.add(key);
      },
      grow(run, key, bytes) {
        w.node.memory.grow(run, key, bytes);
      },
      release(key) {
        w.node.memory.release(key);
        u.holds.delete(key);
      },
    };
  }

  /** The split is done: what it still holds (a sink stopped early) is freed. */
  close(): void {
    for (const k of this.holds) this.w.node.memory.release(k);
    this.holds.clear();
  }

  /** The split failed: undo it, so it can run again from scratch. */
  abort(): void {
    this.close();
    for (const piece of this.written) {
      this.w.node.memory.release(holdKeyOf(piece.ref));
      this.w.node.blocks.drop(piece.block.id);
    }
    this.w.buffers.drop(this.written);
    this.written.length = 0;
  }
}
