// kostka's engine, as the shared layers see it (execution/engine.ts): it plans the
// optimized plan into pipelines and output buffers (physical/planner.ts) and runs it
// with a coordinator and one worker per node (sim.ts). Its settings are the
// planner's: how rows move between workers.

import { KostkaError } from "../../core/errors.ts";
import { planIds } from "../../sql/plan.ts";
import type { Engine, EngineStart, StepKindInfo } from "../../execution/engine.ts";
import { plan } from "./physical/planner.ts";
import type { PlannerSettings } from "./physical/pipeline.ts";
import { SimExecution } from "./sim.ts";

/** kostka's settings, as a query gives them: any of the planner's but `workers` (the
 * cluster's nodes). */
export type KostkaSettings = Partial<Omit<PlannerSettings, "workers">>;

const JOIN_STRATEGIES = ["auto", "broadcast", "shuffle"] as const;

/** Checks settings given as plain values (from a query's options, or the workbench). */
export function kostkaSettings(given: Readonly<Record<string, unknown>>): KostkaSettings {
  const out: { -readonly [K in keyof KostkaSettings]: KostkaSettings[K] } = {};
  for (const [key, v] of Object.entries(given)) {
    if (v === undefined) continue;
    switch (key) {
      case "shufflePartitions":
        if (v === "auto" || (typeof v === "number" && Number.isInteger(v) && v >= 1)) out.shufflePartitions = v;
        else throw new KostkaError(`shufflePartitions must be "auto" or a whole number of at least 1, got ${JSON.stringify(v)}`);
        break;
      case "joinStrategy": {
        const s = JOIN_STRATEGIES.find((x) => x === v);
        if (!s) throw new KostkaError(`joinStrategy must be one of ${JOIN_STRATEGIES.join(", ")}, got ${JSON.stringify(v)}`);
        out.joinStrategy = s;
        break;
      }
      case "runtimeFilters":
        if (typeof v !== "boolean") throw new KostkaError(`runtimeFilters must be true or false, got ${JSON.stringify(v)}`);
        out.runtimeFilters = v;
        break;
      default:
        throw new KostkaError(`kostka has no setting ${JSON.stringify(key)}: its settings are shufflePartitions, joinStrategy and runtimeFilters`);
    }
  }
  return out;
}

/** kostka's kinds of step: who takes them, how the workbench names them. */
export const KOSTKA_STEPS: readonly StepKindInfo[] = [
  { kind: "start", by: "scheduler", label: "submitted", bad: false, doc: "the coordinator gets the plan and the workers; every worker has every pipeline; nothing ran yet" },
  { kind: "plan", by: "scheduler", label: "splits made", bad: false, doc: "a pipeline is ready: the coordinator makes its splits, from data files or by cutting its input buffer's outputs, and gives each partition an owner" },
  { kind: "assign", by: "scheduler", label: "splits assigned", bad: false, doc: "the coordinator sends a worker splits for its queue (AddSplits, at most 2 queued per pipeline), or moves a partition that has not started to an idle worker" },
  { kind: "process", by: "node", label: "split processed", bad: false, doc: "a worker ran a split: the pieces it wrote, hash tables it built or reused, result rows, and SplitDone" },
  { kind: "split_done", by: "scheduler", label: "split reported", bad: false, doc: "the coordinator handles a SplitDone: it records the split's outputs, filter keys and result rows, and refills the worker's queue" },
  { kind: "split_failed", by: "scheduler", label: "split failed", bad: true, doc: "the coordinator handles a SplitFailed: a piece the split read was on a worker that is gone, so it runs again once that output is made again" },
  { kind: "enough", by: "scheduler", label: "enough rows", bad: false, doc: "a Limit's gather buffer holds enough rows: the coordinator skips the producer's pending splits" },
  { kind: "worker_gone", by: "failure", label: "worker gone", bad: true, doc: "a worker is gone: its queued splits go back to pending, its outputs are lost, its partitions get new owners, and the splits that made lost outputs still needed run again" },
  { kind: "pipeline_done", by: "scheduler", label: "pipeline done", bad: false, doc: "every split is done: its output buffer is sealed, runtime filter keys merged, buffers nobody reads any more released, and pipelines waiting for it may start" },
  { kind: "done", by: "scheduler", label: "done", bad: false, doc: "the query is done" },
  { kind: "failed", by: "scheduler", label: "failed", bad: true, doc: "a split failed, or every worker is gone, so the query failed" },
];

export const kostka: Engine = {
  name: "kostka",
  label: "kostka (pipelines)",
  steps: KOSTKA_STEPS,
  unitSteps: ["process"],
  start(q: EngineStart): SimExecution {
    const settings = kostkaSettings(q.settings);
    const physical = plan(q.optimized, q.reader, { ...settings, workers: q.cluster.nodes });
    return new SimExecution(physical, q.cluster, planIds(q.optimized));
  },
};
