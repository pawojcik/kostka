// The TaskScheduler: gives the tasks of submitted stages to free task slots. Each
// stage attempt is a task set: the partitions still to run, the tasks running, and
// how often each partition failed.
//
// - Slots: every executor has one per core of its node (Spark's spark.task.cpus = 1).
//   A slot runs one task at a time.
// - Order: task sets first in, first out, by job and then stage (Spark's FIFO pool);
//   a set's partitions in order. Each task goes to the executor with the most free
//   slots (the lowest id on a tie), in its lowest free slot. There is no data
//   locality: any executor can run any task.
// - A failed task's partition goes back to its set and runs again, with the next
//   attempt number; the caller says whether the failure counts toward
//   maxTaskFailures (a task lost with its executor does not, as in Spark).
// - A zombie set (its stage attempt failed with FetchFailed) launches nothing more;
//   the tasks of it still running may finish.

import { internal } from "../../../core/errors.ts";

export interface TaskSet {
  readonly stage: number;
  readonly stageAttempt: number;
  readonly job: number;
  /** Partitions waiting for a slot, in order. */
  pending: number[];
  /** Tasks running, by tid. */
  readonly running: Set<number>;
  /** Counted failures per partition. */
  readonly failures: Map<number, number>;
  /** The next attempt number per partition. */
  readonly attempts: Map<number, number>;
  zombie: boolean;
}

/** A task to launch: where, and which. */
export interface Launch {
  readonly tid: number;
  readonly executor: number;
  readonly slot: number;
  readonly stage: number;
  readonly stageAttempt: number;
  readonly partition: number;
  readonly attempt: number;
}

interface Running {
  readonly set: TaskSet;
  readonly executor: number;
  readonly slot: number;
  readonly partition: number;
}

export class TaskScheduler {
  /** Executor -> slot -> the tid running in it, or null. */
  private readonly slots: (number | null)[][];
  private readonly up: boolean[];
  private readonly sets: TaskSet[] = [];
  private readonly running = new Map<number, Running>();
  private nextTid = 0;

  constructor(executors: number, slotsEach: number) {
    if (slotsEach < 1) throw internal("an executor needs at least one slot");
    this.slots = Array.from({ length: executors }, () => Array.from({ length: slotsEach }, () => null));
    this.up = Array.from({ length: executors }, () => true);
  }

  /** A new task set: a stage attempt's partitions. */
  submit(stage: number, stageAttempt: number, job: number, partitions: readonly number[]): TaskSet {
    const set: TaskSet = { stage, stageAttempt, job, pending: [...partitions].sort((a, b) => a - b), running: new Set(), failures: new Map(), attempts: new Map(), zombie: false };
    this.sets.push(set);
    this.sets.sort((a, b) => a.job - b.job || a.stage - b.stage || a.stageAttempt - b.stageAttempt);
    return set;
  }

  setOf(stage: number, stageAttempt: number): TaskSet | undefined {
    return this.sets.find((s) => s.stage === stage && s.stageAttempt === stageAttempt);
  }

  freeSlots(executor: number): number {
    return this.up[executor] ? (this.slots[executor]?.filter((t) => t === null).length ?? 0) : 0;
  }

  /** The executors that are up. */
  executorsUp(): number[] {
    return this.up.flatMap((u, i) => (u ? [i] : []));
  }

  /** Gives pending tasks to free slots until either runs out. */
  offer(): Launch[] {
    const out: Launch[] = [];
    for (;;) {
      const set = this.sets.find((s) => !s.zombie && s.pending.length > 0);
      if (!set) break;
      let executor = -1, free = 0;
      for (let e = 0; e < this.slots.length; e++) {
        const f = this.freeSlots(e);
        if (f > free) {
          executor = e;
          free = f;
        }
      }
      if (executor < 0) break;
      const slots = this.slots[executor]!; // `executor` indexes slots
      const slot = slots.indexOf(null);
      const partition = set.pending.shift()!; // `set` has pending partitions
      const attempt = set.attempts.get(partition) ?? 0;
      set.attempts.set(partition, attempt + 1);
      const tid = this.nextTid++;
      slots[slot] = tid;
      set.running.add(tid);
      this.running.set(tid, { set, executor, slot, partition });
      out.push({ tid, executor, slot, stage: set.stage, stageAttempt: set.stageAttempt, partition, attempt });
    }
    return out;
  }

  private end(tid: number): Running | undefined {
    const r = this.running.get(tid);
    if (!r) return undefined;
    this.running.delete(tid);
    r.set.running.delete(tid);
    const slots = this.slots[r.executor];
    if (slots && slots[r.slot] === tid) slots[r.slot] = null;
    return r;
  }

  /** A task finished: its slot is free. */
  finished(tid: number): void {
    this.end(tid);
  }

  /** A task failed: its slot is free and, unless its set is a zombie, its partition
   * runs again. Returns the partition's counted failures so far. */
  failed(tid: number, counted: boolean): number {
    const r = this.end(tid);
    if (!r) return 0;
    const n = (r.set.failures.get(r.partition) ?? 0) + (counted ? 1 : 0);
    r.set.failures.set(r.partition, n);
    if (!r.set.zombie) this.requeue(r.set, r.partition);
    return n;
  }

  private requeue(set: TaskSet, partition: number): void {
    if (set.pending.includes(partition)) return;
    set.pending.push(partition);
    set.pending.sort((a, b) => a - b);
  }

  /** An executor is gone: its slots with it. Its running tasks run again elsewhere
   * (not counted as failures); returns their tids. */
  executorLost(executor: number): number[] {
    this.up[executor] = false;
    const lost = [...this.running.entries()].filter(([, r]) => r.executor === executor).map(([tid]) => tid);
    for (const tid of lost) this.failed(tid, false);
    return lost;
  }

  /** A stage attempt failed: its set launches nothing more. */
  makeZombie(set: TaskSet): void {
    set.zombie = true;
    set.pending = [];
  }

  /** A stage is complete: no more of its tasks are launched. */
  cancel(stage: number): void {
    for (const s of this.sets) if (s.stage === stage) s.pending = [];
  }

  /** Every set: the scheduler fails no query itself; it stops launching when told. */
  stopAll(): void {
    for (const s of this.sets) s.pending = [];
  }

  /** A set with nothing pending and nothing running. */
  over(set: TaskSet): boolean {
    return set.pending.length === 0 && set.running.size === 0;
  }

  /** The task in each slot of an executor (null: free). */
  slotsOf(executor: number): readonly (number | null)[] {
    return this.slots[executor] ?? [];
  }

  isRunning(tid: number): boolean {
    return this.running.has(tid);
  }
}
