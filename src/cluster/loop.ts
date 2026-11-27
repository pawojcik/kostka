// The discrete-event loop every engine runs on, and the history it keeps.
// Infrastructure: it knows nothing of what the engine's scheduler decides or what
// its units compute.
//
// - Transport: events (from a node's unit, or from the infrastructure) go through a
//   queue ordered by simulated time; the engine's scheduler gets them in the order
//   they happened. Commands reach their node at once (the engine delivers them).
// - Executor: each core of each node runs one unit at a time. A unit can start at the
//   later of its core's clock and the time it arrived. The loop always does the
//   earliest thing that can happen: delivering an event or starting a unit, an event
//   first on a tie, and on a tie between units the first the engine offers.
// - Failures: `stop(node)` stops a node just before the next thing happens; the events
//   it sent that were not delivered yet are lost with it.
//
// The history is a list of steps: a scheduler's decision, or a unit run. Each step
// keeps its messages and a frame of every node right after it; a node the step did
// not touch shares its frame with the step before.

import { internal } from "../core/errors.ts";
import type { Node } from "./node.ts";

/** One message of a step: a command or an event, as the log shows it. */
export interface LoggedMessage {
  /** "lost": an event a node sent that was never delivered (it was gone). */
  readonly dir: "command" | "event" | "lost";
  readonly type: string;
  /** "coordinator", "simulation" or a worker ("W0"), in the engine's words. */
  readonly from: string;
  readonly to: string;
  readonly ms: number;
  readonly text: string;
}

/** A step of the history. */
export interface Step {
  readonly seq: number;
  /** Simulated time, ms. */
  readonly ms: number;
  /** The engine's kind of step ("process", "assign"). */
  readonly kind: string;
  readonly text: string;
  /** The node it happened on, or null for the scheduler. */
  readonly node: number | null;
  /** What it is about, in the engine's terms: { pipeline: 2, split: 14 }. */
  readonly refs: Readonly<Record<string, number>>;
  readonly messages: readonly LoggedMessage[];
}

/** A unit a core could start: which node and core, and when it arrived. */
export interface Runnable {
  readonly node: number;
  readonly core: number;
  /** Simulated ns. */
  readonly arrived: number;
}

/** What the loop needs of an engine. */
export interface LoopPorts<E, R extends Runnable> {
  /** The scheduler handles an event that happened at `at`. */
  deliver(event: E, at: number): void;
  /** The units that could start, at most one per core of each node that is up, in
   * the order to prefer on a tie. */
  runnable(): readonly R[];
  /** Runs a unit from `start` (on its node's core). */
  run(unit: R, start: number): Promise<void>;
}

/** The next thing that can happen. */
export type Action<E, R extends Runnable> =
  | { readonly kind: "deliver"; readonly at: number; readonly event: E }
  | { readonly kind: "unit"; readonly at: number; readonly unit: R };

interface Queued<E> {
  readonly at: number;
  readonly n: number;
  readonly from: number | null;
  readonly event: E;
}

export class EventLoop<E, R extends Runnable> {
  readonly nodes: readonly Node[];
  private readonly ports: LoopPorts<E, R>;
  private readonly queue: Queued<E>[] = [];
  private sent = 0;

  constructor(nodes: readonly Node[], ports: LoopPorts<E, R>) {
    this.nodes = nodes;
    this.ports = ports;
  }

  /** Queues an event that happened at `at`, sent by node `from` (null: by the
   * infrastructure), for the scheduler. */
  send(at: number, from: number | null, event: E): void {
    this.queue.push({ at, n: this.sent++, from, event });
    this.queue.sort((a, b) => a.at - b.at || a.n - b.n);
  }

  /** The earliest thing that can happen, without doing it. */
  peek(): Action<E, R> | null {
    let unit: { kind: "unit"; at: number; unit: R } | null = null;
    for (const r of this.ports.runnable()) {
      const node = this.nodes[r.node];
      const clock = node?.clocks[r.core];
      if (!node || clock === undefined) throw internal(`there is no core ${r.core} on node ${r.node}`);
      if (!node.up) continue;
      const at = Math.max(clock, r.arrived);
      if (!unit || at < unit.at) unit = { kind: "unit", at, unit: r };
    }
    const d = this.queue[0];
    if (d && (!unit || d.at <= unit.at)) return { kind: "deliver", at: d.at, event: d.event };
    return unit;
  }

  /** Does the earliest thing that can happen. False when nothing can. */
  async advance(): Promise<boolean> {
    const a = this.peek();
    if (!a) return false;
    if (a.kind === "deliver") {
      this.queue.shift();
      this.ports.deliver(a.event, a.at);
    } else await this.ports.run(a.unit, a.at);
    return true;
  }

  /** Stops node `n` just before the next thing happens (or at `idle` if nothing can);
   * returns when that is, and the events it sent that are lost with it. */
  stop(n: number, idle: number): { at: number; lost: { at: number; event: E }[] } {
    const node = this.nodes[n];
    if (!node) throw new RangeError(`there is no node ${n}`);
    if (!node.up) throw new RangeError(`node ${n} is gone already`);
    const next = this.peek();
    const at = next ? next.at : idle;
    node.up = false;
    const lost: { at: number; event: E }[] = [];
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const q = this.queue[i];
      if (!q || q.from !== n) continue;
      this.queue.splice(i, 1);
      lost.unshift({ at: q.at, event: q.event });
    }
    return { at, lost };
  }

  /** When the busiest node is free again, in simulated ns. */
  get elapsed(): number {
    return Math.max(0, ...this.nodes.map((m) => m.clock));
  }

  /** The event the scheduler gets next, if any. */
  nextEvent(): E | null {
    return this.queue[0]?.event ?? null;
  }
}

/** The steps so far, with every node's frame right after each. `F` is the engine's
 * frame of a node; it says whether the node was up. */
export class History<F extends { readonly status: "up" | "gone" }> {
  readonly steps: Step[] = [];
  private readonly frames: (readonly F[])[] = [];
  private readonly nodes: readonly Node[];
  private readonly frameOf: (node: Node) => F;

  constructor(nodes: readonly Node[], frameOf: (node: Node) => F) {
    this.nodes = nodes;
    this.frameOf = frameOf;
  }

  /** Records a step; the nodes it `touched`, and any node gone since the last step,
   * get a new frame. */
  record(step: Omit<Step, "seq">, touched: ReadonlySet<number>): Step {
    const s: Step = { seq: this.steps.length, ...step };
    this.steps.push(s);
    const prev = this.frames[this.frames.length - 1];
    this.frames.push(this.nodes.map((node, i) => {
      const before = prev?.[i];
      return !before || touched.has(i) || (before.status === "up" && !node.up) ? this.frameOf(node) : before;
    }));
    return s;
  }

  /** Every node's frame right after step `at`. */
  framesAt(at: number): readonly F[] {
    const f = this.frames[at];
    if (!f) throw internal(`there is no step ${at}`);
    return f;
  }
}
