// A toy engine, registered only in tests, to show that the shared layers (the API, the
// Session, the cluster) run an engine with no kostka code involved (ADR 0001).
//
// It ignores the plan: it runs one unit per node, unit k on node k, and unit k returns
// the fixed row [k]. A unit takes 1 ms of CPU and reads 1 KB from storage. When a node
// stops, the units it had not reported fail, and run again on the next node that is up.
// Its steps: "start", "work" (a unit ran), "collect" (the scheduler got its rows),
// "node_gone", "done" and "failed".

import type { Row } from "../../src/core/types.ts";
import type { ClusterConfig } from "../../src/cluster/config.ts";
import { IO_PROFILES } from "../../src/cluster/config.ts";
import { Node, type NodeFrame } from "../../src/cluster/node.ts";
import { MemoryPool } from "../../src/cluster/memory.ts";
import { BlockStore } from "../../src/cluster/blocks.ts";
import { EventLoop, History, type Runnable, type Step } from "../../src/cluster/loop.ts";
import type { Engine, EngineStart, ExecState, Execution, UnitFrame } from "../../src/execution/engine.ts";

interface Done { readonly unit: number; readonly attempt: number }
interface Attempt { unit: number; node: number; attempt: number; arrived: number; status: "waiting" | "ran" | "done" | "failed" }
interface ToyRunnable extends Runnable { readonly attempt: Attempt }

class ToyExecution implements Execution {
  readonly engine = "toy";
  private readonly cluster: ClusterConfig;
  private readonly nodes: Node[];
  private readonly loop: EventLoop<Done, ToyRunnable>;
  private readonly history: History<NodeFrame>;
  private readonly attempts: Attempt[] = [];
  private readonly units: UnitFrame[] = [];
  private readonly results = new Map<number, Row>();
  private over = false;
  private failure: string | null = null;

  constructor(cluster: ClusterConfig) {
    this.cluster = cluster;
    const profile = IO_PROFILES[cluster.io];
    if (!profile) throw new RangeError(`no IO profile ${cluster.io}`);
    this.nodes = Array.from({ length: cluster.nodes }, (_, i) => new Node(i, cluster.cores, profile, new MemoryPool(`N${i}`, cluster.memoryMB * 1024 * 1024), new BlockStore(i)));
    for (let i = 0; i < cluster.nodes; i++) this.attempts.push({ unit: i, node: i, attempt: 1, arrived: 0, status: "waiting" });
    this.history = new History(this.nodes, (n) => n.frame());
    this.loop = new EventLoop<Done, ToyRunnable>(this.nodes, {
      runnable: () => this.nodes.filter((n) => n.up).flatMap((n) => {
        const a = this.attempts.find((x) => x.node === n.id && x.status === "waiting");
        return a ? [{ node: n.id, core: 0, arrived: a.arrived, attempt: a }] : [];
      }),
      run: async (r, start) => this.work(r.attempt, start),
      deliver: (e, at) => this.collect(e, at),
    });
    this.record("start", `one unit on each of ${cluster.nodes} nodes`, 0, null, {});
  }

  private record(kind: string, text: string, ns: number, node: number | null, refs: Record<string, number>, touched: number[] = []): void {
    this.history.record({ ms: ns / 1e6, kind, text, node, refs, messages: [] }, new Set(touched));
  }

  private work(a: Attempt, start: number): void {
    const node = this.nodes[a.node]!; // the loop only runs units of nodes it has
    const u = node.begin(0, start);
    u.cpu(1e6);
    u.io("storage", 1024, "storage_read");
    const t = u.end(true);
    a.status = "ran";
    this.units.push({ id: this.units.length, node: a.node, core: 0, start_ms: t.start / 1e6, end_ms: t.end / 1e6, label: `unit ${a.unit}`, group: "all", failed: null, segments: t.segments.map(([k, s, e]) => [k, s / 1e6, e / 1e6] as const) });
    this.record("work", `N${a.node} ran unit ${a.unit}`, t.end, a.node, { unit: a.unit }, [a.node]);
    this.loop.send(t.end, a.node, { unit: a.unit, attempt: a.attempt });
  }

  private collect(e: Done, at: number): void {
    this.attempts.find((a) => a.unit === e.unit && a.attempt === e.attempt)!.status = "done"; // only attempts send Done
    this.results.set(e.unit, [e.unit]);
    this.record("collect", `the scheduler got unit ${e.unit}'s rows`, at, null, { unit: e.unit });
    if (this.results.size === this.cluster.nodes) {
      this.record("done", "the query is done", at, null, {});
      this.over = true;
    }
  }

  get done(): boolean {
    return this.over && this.failure === null;
  }

  async step(): Promise<boolean> {
    if (this.over) return false;
    if (await this.loop.advance()) return true;
    this.fail("nothing can run", this.loop.elapsed);
    return true;
  }

  private fail(why: string, ns: number): void {
    this.failure = why;
    this.over = true;
    this.record("failed", `the query failed: ${why}`, ns, null, {});
  }

  async wave(): Promise<number> {
    const seen = new Set<number>();
    let n = 0;
    while (!this.over) {
      const a = this.loop.peek();
      if (a?.kind === "unit" && seen.has(a.unit.node)) break;
      if (a?.kind === "unit") seen.add(a.unit.node);
      await this.step();
      n++;
    }
    return n;
  }

  async runConcurrent(onProgress?: () => void): Promise<void> {
    await this.run(onProgress).catch(() => undefined);
  }

  async run(onProgress?: () => void): Promise<Row[]> {
    while (await this.step()) onProgress?.();
    return this.result();
  }

  stopNode(n: number): void {
    const last = this.history.steps[this.history.steps.length - 1];
    const { at } = this.loop.stop(n, (last?.ms ?? 0) * 1e6);
    const up = this.nodes.filter((x) => x.up);
    for (const a of this.attempts.filter((x) => x.node === n && (x.status === "waiting" || x.status === "ran"))) {
      a.status = "failed";
      const u = this.units.findLast((x) => x.label === `unit ${a.unit}` && x.node === n);
      if (u) this.units[u.id] = { ...u, failed: `N${n} is gone` };
      const next = up[a.unit % Math.max(1, up.length)];
      if (next) this.attempts.push({ unit: a.unit, node: next.id, attempt: a.attempt + 1, arrived: at, status: "waiting" });
    }
    this.record("node_gone", `N${n} is gone`, at, n, {}, [n]);
    if (!up.length) this.fail("every node is gone", at);
  }

  result(): Row[] {
    if (this.failure) throw new Error(this.failure);
    if (!this.done) throw new Error("the query is not done yet");
    return this.rows().slice();
  }

  rows(): readonly Row[] {
    return [...this.results.keys()].sort((a, b) => a - b).map((k) => this.results.get(k)!); // keys come from the map
  }

  state(at = this.history.steps.length - 1): ExecState {
    const event: Step | undefined = this.history.steps[at];
    if (!event) throw new RangeError(`there is no step ${at}`);
    const steps = this.history.steps.slice(0, at + 1);
    const units = this.units.filter((_, i) => i < steps.filter((s) => s.kind === "work").length);
    const nodes = this.history.framesAt(at);
    return {
      engine: this.engine, at, events: this.history.steps.length, event,
      done: steps.some((s) => s.kind === "done"),
      failed: steps.some((s) => s.kind === "failed") && this.failure ? { unit: "scheduler", error: this.failure } : null,
      elapsed_ms: Math.max(event.ms, ...nodes.map((x) => x.clock_ms)),
      next: null, cluster: this.cluster, nodes, units,
      result_rows: steps.filter((s) => s.kind === "collect").length,
    };
  }

  explain(): string {
    return `toy: one unit on each of ${this.cluster.nodes} nodes`;
  }

  get events(): readonly Step[] {
    return this.history.steps;
  }

  progress(): { done: number; total: number } {
    return { done: this.results.size, total: this.cluster.nodes };
  }

  get elapsedNs(): number {
    return this.loop.elapsed;
  }
}

export const toy: Engine = {
  name: "toy",
  label: "toy (one unit per node)",
  steps: [
    { kind: "start", by: "scheduler", label: "started", doc: "the query starts", bad: false },
    { kind: "work", by: "node", label: "unit ran", doc: "a node ran a unit", bad: false },
    { kind: "collect", by: "scheduler", label: "rows collected", doc: "the scheduler got a unit's rows", bad: false },
    { kind: "node_gone", by: "failure", label: "node gone", doc: "a node stopped", bad: true },
    { kind: "done", by: "scheduler", label: "done", doc: "the query is done", bad: false },
    { kind: "failed", by: "scheduler", label: "failed", doc: "the query failed", bad: true },
  ],
  unitSteps: ["work"],
  start: (q: EngineStart) => new ToyExecution(q.cluster),
};
