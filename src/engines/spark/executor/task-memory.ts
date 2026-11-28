// A running task's memory: a fixed share of its node's memory, memoryMB / cores, so
// that every slot's task can hold as much at once. (Spark's unified memory manager
// lets tasks borrow from each other; a fixed share makes "more cores, less memory per
// task" plain.) Each operator of the task says how much it holds; `reserve` refuses a
// size that would take the task past its share, and the operator spills (a Sort) or
// fails (a HashAggregate).
//
// What it holds is also held in the node's memory, so the node's frame shows it; the
// node can still refuse it (OutOfMemoryError) if broadcast values fill the node.

import type { NodeMemory, SpillSink } from "./ports.ts";

export class TaskMemory {
  /** The bytes this task may hold. */
  readonly share: number;
  private readonly held = new Map<number, number>();
  private used = 0;
  peak = 0;
  private readonly node: NodeMemory;
  private readonly sink: SpillSink;
  /** The prefix of its keys in the node's memory, and of its labels ("S2 task 3.0"). */
  private readonly key: string;
  private readonly label: string;

  constructor(share: number, node: NodeMemory, sink: SpillSink, key: string, label: string) {
    this.share = share;
    this.node = node;
    this.sink = sink;
    this.key = key;
    this.label = label;
  }

  /** Grows what `op` holds to `bytes`; false if the task's share would be passed (it
   * then holds what it held before). */
  reserve(op: number, what: string, bytes: number): boolean {
    const before = this.held.get(op) ?? 0;
    if (bytes <= before) return true;
    if (this.used - before + bytes > this.share) return false;
    const key = `${this.key}:op${op}`;
    if (this.held.has(op)) this.node.grow(this.sink, key, bytes);
    else this.node.hold(this.sink, key, "task", `${this.label} ${what}`, bytes, null, "Give each node more memory, or use fewer cores (each task gets memory / cores).");
    this.held.set(op, bytes);
    this.used += bytes - before;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }

  free(op: number): void {
    const b = this.held.get(op);
    if (b === undefined) return;
    this.node.release(`${this.key}:op${op}`);
    this.held.delete(op);
    this.used -= b;
  }

  /** The task ended: everything it held is freed. */
  close(): void {
    for (const op of [...this.held.keys()]) this.free(op);
  }
}
