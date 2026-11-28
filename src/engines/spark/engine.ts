// The Spark-style engine, as the shared layers see it (execution/engine.ts): it plans
// the optimized plan into a Spark plan (physical/planner.ts), cuts it into jobs and
// stages (physical/stages.ts), and runs them with a driver and one executor per node
// (sim.ts). Its settings are Spark's: when to broadcast a join, how many partitions a
// shuffle has, and how often a task may fail.

import { KostkaError } from "../../core/errors.ts";
import { planIds } from "../../sql/plan.ts";
import type { Engine, EngineStart, StepKindInfo } from "../../execution/engine.ts";
import { planSpark } from "./physical/planner.ts";
import { cutStages } from "./physical/stages.ts";
import { SparkExecution } from "./sim.ts";

export interface SparkSettings {
  /** Bytes: a join's build side estimated at most this big is broadcast; -1: never. */
  readonly broadcastThreshold: number;
  /** Partitions of a hash Exchange; "auto" is 2 per core in the cluster. */
  readonly shufflePartitions: number | "auto";
  /** Attempts of one task before the query fails. */
  readonly maxTaskFailures: number;
}

export const SPARK_DEFAULTS: SparkSettings = { broadcastThreshold: 32 * 1024, shufflePartitions: "auto", maxTaskFailures: 4 };

/** The partitions of a hash Exchange: "auto" is nodes × cores × 2. */
export function sparkPartitions(s: Pick<SparkSettings, "shufflePartitions">, nodes: number, cores: number): number {
  return s.shufflePartitions === "auto" ? nodes * cores * 2 : s.shufflePartitions;
}

/** Checks settings given as plain values (a query's options, or the workbench). */
export function sparkSettings(given: Readonly<Record<string, unknown>>): SparkSettings {
  const out: { -readonly [K in keyof SparkSettings]: SparkSettings[K] } = { ...SPARK_DEFAULTS };
  for (const [key, v] of Object.entries(given)) {
    if (v === undefined) continue;
    switch (key) {
      case "broadcastThreshold":
        if (typeof v === "number" && Number.isInteger(v) && v >= -1) out.broadcastThreshold = v;
        else throw new KostkaError(`broadcastThreshold must be a whole number of bytes, or -1 to turn broadcasts off; got ${JSON.stringify(v)}`);
        break;
      case "shufflePartitions":
        if (v === "auto" || (typeof v === "number" && Number.isInteger(v) && v >= 1)) out.shufflePartitions = v;
        else throw new KostkaError(`shufflePartitions must be "auto" or a whole number of at least 1, got ${JSON.stringify(v)}`);
        break;
      case "maxTaskFailures":
        if (typeof v === "number" && Number.isInteger(v) && v >= 1) out.maxTaskFailures = v;
        else throw new KostkaError(`maxTaskFailures must be a whole number of at least 1, got ${JSON.stringify(v)}`);
        break;
      default:
        throw new KostkaError(`the Spark-style engine has no setting ${JSON.stringify(key)}: its settings are broadcastThreshold, shufflePartitions and maxTaskFailures`);
    }
  }
  return out;
}

/** Its kinds of step, named as Spark's listener events are. */
export const SPARK_STEPS: readonly StepKindInfo[] = [
  { kind: "SQLExecutionStart", by: "scheduler", label: "SQLExecutionStart", bad: false, doc: "the query is submitted: the driver has its jobs and stages, and every executor has the plan; nothing ran yet" },
  { kind: "JobStart", by: "scheduler", label: "JobStart", bad: false, doc: "a job starts: the query's result, or a broadcast that a later job reads; a job starts once the broadcasts it needs are made" },
  { kind: "StageSubmitted", by: "scheduler", label: "StageSubmitted", bad: false, doc: "a stage is submitted: every parent stage is done, so its tasks can be given to free slots. Stages are barriers" },
  { kind: "TaskStart", by: "scheduler", label: "TaskStart", bad: false, doc: "the driver launches a task in a free slot (LaunchTask): the executor with the most free slots first" },
  { kind: "TaskRan", by: "node", label: "task ran", bad: false, doc: "an executor ran a task in a slot, on that slot's core: what it read and wrote; it sends TaskFinished or TaskFailed" },
  { kind: "TaskEnd", by: "scheduler", label: "TaskEnd", bad: false, doc: "the driver handles TaskFinished (a map task's MapStatus is registered, or a result task's rows kept) or TaskFailed (the partition runs again, up to maxTaskFailures)" },
  { kind: "StageCompleted", by: "scheduler", label: "StageCompleted", bad: false, doc: "every partition of a stage has its output: waiting stages may be submitted, and shuffles and broadcasts nobody reads any more are removed" },
  { kind: "JobEnd", by: "scheduler", label: "JobEnd", bad: false, doc: "a job's result stage is done: a broadcast is made (executors fetch it from the driver), or the query's result is in" },
  { kind: "ExecutorLost", by: "failure", label: "ExecutorLost", bad: true, doc: "a node is gone with its executor: its running tasks run again elsewhere, and its shuffle files are lost" },
  { kind: "FetchFailed", by: "failure", label: "FetchFailed", bad: true, doc: "a task could not read a shuffle file (its node is gone): its stage attempt fails, and the parent stage is resubmitted for the missing partitions" },
  { kind: "StageResubmitted", by: "scheduler", label: "StageResubmitted", bad: true, doc: "a stage runs again as a new attempt, for the partitions whose output is missing" },
  { kind: "SQLExecutionEnd", by: "scheduler", label: "SQLExecutionEnd", bad: false, doc: "the query is done" },
  { kind: "SQLExecutionFailed", by: "scheduler", label: "failed", bad: true, doc: "a task failed maxTaskFailures times, a stage was tried too often, or every node is gone, so the query failed" },
];

export const spark: Engine = {
  name: "spark",
  label: "Spark-style (stages and tasks)",
  steps: SPARK_STEPS,
  unitSteps: ["TaskRan"],
  start(q: EngineStart): SparkExecution {
    const settings = sparkSettings(q.settings);
    const plan = planSpark(q.optimized, q.reader, {
      broadcastThreshold: settings.broadcastThreshold,
      shufflePartitions: sparkPartitions(settings, q.cluster.nodes, q.cluster.cores),
    });
    return new SparkExecution(plan, cutStages(plan), q.cluster, settings, planIds(q.optimized));
  },
};
