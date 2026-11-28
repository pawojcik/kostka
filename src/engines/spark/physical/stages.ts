// The cut of a Spark plan into jobs and stages, as Spark's DAGScheduler makes them.
//
// - A job runs one action. The query's result is one job; each BroadcastExchange is
//   a job of its own (its rows are collected by the driver), and a job that reads a
//   broadcast starts only once the broadcast's job is done. A broadcast job gets a
//   lower id than the jobs that need it.
// - A job is cut into stages at every ShuffleExchange. A ShuffleMapStage computes the
//   exchange's child and writes the shuffle; the ResultStage computes the job's top
//   operator and sends its rows to the driver. A stage's parents are the map stages
//   of the shuffles it reads; they get lower ids.
// - A stage has one task per partition of its top operator: one per file in a scan
//   stage, `shufflePartitions` where it reads a hash Exchange, one where it reads a
//   SinglePartition one.
//
// The operators of a stage run in one task, batch by batch: Spark's whole-stage code
// generation would compile them into one function, a WholeStageCodegen box. Nothing
// is generated here; `ops` is the box.

import { internal } from "../../../core/errors.ts";
import { BroadcastExchange, ShuffleExchange } from "./operators/exchange.ts";
import { FileScan } from "./operators/scan.ts";
import { partitionCount, type SparkOp } from "./plan.ts";
import type { SparkPlan } from "./planner.ts";

export interface Stage {
  readonly id: number;
  /** ShuffleMapStage ("map") or ResultStage ("result"). */
  readonly kind: "map" | "result";
  readonly job: number;
  /** The operator whose partitions its tasks compute. */
  readonly top: SparkOp;
  /** The shuffle a map stage writes (null: a result stage). */
  readonly writes: ShuffleExchange | null;
  /** The map stages of the shuffles it reads. */
  readonly parents: readonly number[];
  /** The shuffles it reads, by shuffle id. */
  readonly reads: readonly number[];
  /** The broadcasts its joins read, by broadcast id. */
  readonly broadcasts: readonly number[];
  readonly partitions: number;
  /** Its operators, top first: its WholeStageCodegen box. */
  readonly ops: readonly SparkOp[];
  readonly scans: readonly FileScan[];
}

export interface Job {
  readonly id: number;
  /** The broadcast it makes, or null for the job of the query's result. */
  readonly broadcast: BroadcastExchange | null;
  /** Its stages, parents first; the last is its result stage. */
  readonly stages: readonly number[];
  /** The jobs whose broadcasts its stages read: they run first. */
  readonly needs: readonly number[];
}

export class StagePlan {
  readonly jobs: readonly Job[];
  readonly stages: readonly Stage[];
  /** The stage that writes each shuffle, by shuffle id. */
  readonly writer: ReadonlyMap<number, number>;
  /** The job that makes each broadcast, by broadcast id. */
  readonly maker: ReadonlyMap<number, number>;
  constructor(jobs: readonly Job[], stages: readonly Stage[]) {
    this.jobs = jobs;
    this.stages = stages;
    this.writer = new Map(stages.flatMap((s) => (s.writes ? [[s.writes.shuffle, s.id] as const] : [])));
    this.maker = new Map(jobs.flatMap((j) => (j.broadcast ? [[j.broadcast.broadcast, j.id] as const] : [])));
  }
  /** The job of the query's result: the last one. */
  get resultJob(): Job {
    const j = this.jobs[this.jobs.length - 1];
    if (!j) throw internal("a stage plan has at least one job");
    return j;
  }
  stage(id: number): Stage {
    const s = this.stages[id];
    if (!s) throw internal(`there is no stage ${id}`);
    return s;
  }
  job(id: number): Job {
    const j = this.jobs[id];
    if (!j) throw internal(`there is no job ${id}`);
    return j;
  }
  /** The result stage of a job. */
  resultStage(job: number): Stage {
    const id = this.job(job).stages.at(-1);
    if (id === undefined) throw internal(`job ${job} has no stages`);
    return this.stage(id);
  }
  /** The stages that read the shuffle a stage writes. */
  children(stage: number): Stage[] {
    const w = this.stage(stage).writes;
    return w ? this.stages.filter((s) => s.reads.includes(w.shuffle)) : [];
  }
}

export function cutStages(plan: SparkPlan): StagePlan {
  const jobs: Job[] = [];
  const stages: Stage[] = [];
  const jobOf = new Map<BroadcastExchange, number>();

  const makeStage = (top: SparkOp, writes: ShuffleExchange | null, job: number): number => {
    const parents: number[] = [], reads: number[] = [], broadcasts: number[] = [], ops: SparkOp[] = [], scans: FileScan[] = [];
    const visit = (op: SparkOp): void => {
      ops.push(op);
      if (op instanceof FileScan) scans.push(op);
      for (const c of op.children()) {
        if (c instanceof ShuffleExchange) {
          parents.push(makeStage(c.child, c, job));
          reads.push(c.shuffle);
        } else if (c instanceof BroadcastExchange) broadcasts.push(c.broadcast);
        else visit(c);
      }
    };
    visit(top);
    const partitions = partitionCount(top.partitioning());
    for (const r of reads) {
      const ex = plan.shuffles[r];
      if (!ex || ex.n !== partitions) throw internal(`a stage of ${partitions} partition(s) reads shuffle ${r} of ${ex?.n} partition(s)`);
    }
    for (const s of scans) {
      if (partitionCount(s.partitioning()) !== partitions) throw internal(`a stage of ${partitions} partition(s) scans ${s.label()}`);
    }
    const id = stages.length;
    stages.push({ id, kind: writes ? "map" : "result", job, top, writes, parents, reads, broadcasts, partitions, ops, scans });
    return id;
  };

  const makeJob = (top: SparkOp, broadcast: BroadcastExchange | null): number => {
    // The broadcasts its stages read are jobs of their own, made first.
    const needs: number[] = [];
    const visit = (op: SparkOp): void => {
      for (const c of op.children()) {
        if (c instanceof BroadcastExchange) {
          let j = jobOf.get(c);
          if (j === undefined) {
            j = makeJob(c.child, c);
            jobOf.set(c, j);
          }
          if (!needs.includes(j)) needs.push(j);
        } else visit(c);
      }
    };
    visit(top);
    const id = jobs.length;
    const first = stages.length;
    makeStage(top, null, id);
    jobs.push({ id, broadcast, stages: stages.slice(first).map((s) => s.id), needs });
    return id;
  };

  makeJob(plan.root, null);
  return new StagePlan(jobs, stages);
}
