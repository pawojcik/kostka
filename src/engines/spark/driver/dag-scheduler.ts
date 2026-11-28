// The DAGScheduler: the driver's brain, one per query. It knows the jobs and stages
// (physical/stages.ts), runs them in order, and recovers from failures. It reads no
// rows but the result's (and the broadcast jobs', which it keeps to hand out).
//
// - Jobs: a job starts once the broadcasts it reads are made; jobs that need nothing
//   start at once, side by side. The query is done when the result job is.
// - Stages: a stage is submitted when every parent stage's shuffle output is
//   available, and not before: stages are barriers. A submitted stage attempt is a
//   task set for the TaskScheduler: the partitions still missing. A map stage is
//   complete when every map partition has an output (MapOutputTracker); a result
//   stage when every partition's rows are in.
// - Shuffle files and broadcast copies are freed (RemoveShuffle, RemoveBroadcast)
//   once every stage that reads them is done.
//
// Failures:
// - A task error (out of memory, out of disk): the partition runs again, up to
//   maxTaskFailures attempts in a stage attempt; then the query fails, naming the task.
// - ExecutorLost: its running tasks run again elsewhere (this does not count as a
//   failure), and its shuffle outputs are lost: the stages that wrote them are no
//   longer complete. A stage submitted later sees its parent's missing outputs and
//   resubmits the parent first.
// - FetchFailed: a task of a running stage could not read a shuffle file. That stage
//   attempt fails (a zombie: it launches nothing more), the parent stage is resubmitted
//   for its missing partitions only, and the failed stage runs again after it. A stage
//   is tried at most maxStageAttempts times.
// - No executor left: the query fails.
//
// Each decision is a step (`ports.decide`) with the commands it sends; every change it
// makes remembers its step, so the workbench can show the driver's state after any.

import { History } from "../../../core/history.ts";
import { internal } from "../../../core/errors.ts";
import type { Row } from "../../../core/types.ts";
import type { Stage, StagePlan } from "../physical/stages.ts";
import { describeMessage, taskName, type Command, type DriverEvent, type ExecutorEvent, type TaskId, type TaskMetrics } from "../protocol.ts";
import { MapOutputTracker } from "./map-output-tracker.ts";
import { TaskScheduler, type Launch } from "./task-scheduler.ts";

export type SparkStepKind =
  | "SQLExecutionStart" | "JobStart" | "StageSubmitted" | "TaskStart" | "TaskEnd" | "StageCompleted" | "JobEnd"
  | "ExecutorLost" | "FetchFailed" | "StageResubmitted" | "SQLExecutionEnd" | "SQLExecutionFailed";

export interface Refs {
  readonly job?: number;
  readonly stage?: number;
  readonly tid?: number;
  readonly executor?: number;
  readonly partition?: number;
}

export interface Decision {
  readonly kind: SparkStepKind;
  readonly text: string;
  readonly refs: Refs;
  readonly commands: readonly Command[];
}

export interface DriverPorts {
  /** A decision: a step, with the commands it sends (which reach their executors now). */
  decide(d: Decision, now: number): void;
  /** The number the next step will have. */
  seq(): number;
}

/** How many times a stage is tried (Spark's spark.stage.maxConsecutiveAttempts). */
export const MAX_STAGE_ATTEMPTS = 4;

export interface DriverSettings {
  readonly executors: number;
  readonly slots: number;
  readonly maxTaskFailures: number;
  readonly maxStageAttempts: number;
}

export type JobStatus = "waiting" | "running" | "succeeded" | "failed";
/** waiting: not submitted, or waiting for its parents again; running: an attempt is. */
export type StageStatus = "waiting" | "running" | "done";
/** lost: its executor was gone before it reported. */
export type TaskStatus = "running" | "success" | "failed" | "lost";

export class JobState {
  readonly id: number;
  readonly status: History<JobStatus>;
  start: number | null = null;
  end: number | null = null;
  constructor(id: number) {
    this.id = id;
    this.status = new History<JobStatus>(0, "waiting");
  }
}

export class StageState {
  readonly stage: Stage;
  readonly status: History<StageStatus>;
  /** The latest attempt (-1: never submitted). */
  readonly attempt: History<number>;
  /** Why it is in its status, in a few words. */
  readonly why: History<string>;
  /** Each attempt: when it was submitted, and the partitions it was for. */
  readonly attempts: { attempt: number; seq: number; at: number; partitions: readonly number[] }[] = [];
  /** A result stage's partitions whose rows are in, with them. */
  readonly results = new Map<number, readonly Row[]>();
  start: number | null = null;
  end: number | null = null;
  constructor(stage: Stage) {
    this.stage = stage;
    this.status = new History<StageStatus>(0, "waiting");
    this.attempt = new History(0, -1);
    this.why = new History(0, "not submitted yet");
  }
}

export interface TaskRecord extends TaskId {
  readonly executor: number;
  readonly slot: number;
  /** The step it was launched in, and when. */
  readonly seq: number;
  readonly at: number;
  readonly status: History<TaskStatus>;
  end: number | null;
  error: string | null;
  fetchFailed: boolean;
  metrics: TaskMetrics | null;
}

export class DAGScheduler {
  readonly plan: StagePlan;
  readonly settings: DriverSettings;
  readonly jobs: JobState[];
  readonly stages: StageState[];
  readonly tasks: TaskRecord[] = [];
  readonly tracker: MapOutputTracker;
  readonly scheduler: TaskScheduler;
  readonly executors: History<"up" | "gone">[];
  /** The rows of each broadcast, once its job is done: executors fetch them. */
  private readonly broadcastRows = new Map<number, readonly Row[]>();
  /** Why the query failed: a task failed too often ("task"), a stage was tried too
   * often ("stage"), or no executor is left ("executors"). */
  failed: { unit: string; error: string; seq: number; cause: "task" | "stage" | "executors" } | null = null;
  done = false;
  private readonly ports: DriverPorts;

  constructor(plan: StagePlan, settings: DriverSettings, ports: DriverPorts) {
    this.plan = plan;
    this.settings = settings;
    this.ports = ports;
    this.jobs = plan.jobs.map((j) => new JobState(j.id));
    this.stages = plan.stages.map((s) => new StageState(s));
    const maps: number[] = [];
    for (const s of plan.stages) if (s.writes) maps[s.writes.shuffle] = s.partitions;
    this.tracker = new MapOutputTracker(Array.from({ length: maps.length }, (_, i) => maps[i] ?? 0));
    this.scheduler = new TaskScheduler(settings.executors, settings.slots);
    this.executors = Array.from({ length: settings.executors }, () => new History<"up" | "gone">(0, "up"));
  }

  private get seq(): number {
    return this.ports.seq();
  }

  private decide(kind: SparkStepKind, text: string, refs: Refs = {}, commands: readonly Command[] = []): void {
    this.ports.decide({ kind, text, refs, commands }, this.now);
  }

  /** The time of the event being handled. */
  private now = 0;

  stage(id: number): StageState {
    const s = this.stages[id];
    if (!s) throw internal(`there is no stage ${id}`);
    return s;
  }

  private task(tid: number): TaskRecord {
    const t = this.tasks[tid];
    if (!t) throw internal(`there is no task ${tid}`);
    return t;
  }

  /** The rows of broadcast `b` (its job is done). */
  broadcast(b: number): readonly Row[] {
    const rows = this.broadcastRows.get(b);
    if (!rows) throw internal(`broadcast ${b} is not made yet`);
    return rows;
  }

  /** The result rows so far, in partition order. */
  rows(): Row[] {
    const st = this.stage(this.plan.resultStage(this.plan.resultJob.id).id);
    return [...st.results.entries()].sort((a, b) => a[0] - b[0]).flatMap(([, rows]) => [...rows]);
  }

  // --- starting -------------------------------------------------------------------

  start(now: number): void {
    this.now = now;
    const n = this.plan.jobs.length;
    this.decide("SQLExecutionStart", `the query is submitted: ${n} job${n === 1 ? "" : "s"} (${n - 1} for broadcasts), ${this.plan.stages.length} stage(s); every executor has the plan`);
    this.startJobs();
    this.launch();
  }

  /** Starts every waiting job whose broadcasts are made. */
  private startJobs(): void {
    for (const j of this.plan.jobs) {
      const js = this.jobs[j.id]!; // jobs are made from plan.jobs
      if (js.status.now !== "waiting" || !j.needs.every((x) => this.jobs[x]?.status.now === "succeeded")) continue;
      js.status.set(this.seq, "running");
      js.start = this.now;
      const what = j.broadcast ? `the broadcast for ${j.broadcast.label()} (#${j.broadcast.id})` : "the query's result";
      this.decide("JobStart", `job ${j.id} starts: ${what}; stage${j.stages.length === 1 ? "" : "s"} ${j.stages.join(", ")}`, { job: j.id });
      this.submitStage(this.plan.resultStage(j.id).id);
    }
  }

  /** Every partition's output is there: a map stage's in the tracker, a result
   * stage's rows. */
  private complete(s: StageState): boolean {
    const w = s.stage.writes;
    return w ? this.tracker.available(w.shuffle) : s.results.size === s.stage.partitions;
  }

  /** The parents whose shuffle output is not all available. */
  private missingParents(s: StageState): number[] {
    return s.stage.parents.filter((p) => !this.complete(this.stage(p)));
  }

  /** Submits a stage if its parents are complete; else submits them first. */
  private submitStage(id: number): void {
    if (this.failed) return;
    const s = this.stage(id);
    if (s.status.now === "running" || (s.status.now === "done" && this.complete(s))) return;
    const missing = this.missingParents(s);
    if (!missing.length) return this.submitMissingTasks(s);
    s.status.set(this.seq, "waiting");
    s.why.set(this.seq, `waiting for stage${missing.length === 1 ? "" : "s"} ${missing.join(", ")}`);
    for (const p of missing) this.submitStage(p);
  }

  private submitMissingTasks(s: StageState): void {
    const attempt = s.attempt.now + 1;
    const id = s.stage.id;
    if (attempt >= this.settings.maxStageAttempts) {
      return this.fail("stage", `stage ${id}`, `stage ${id} was tried ${this.settings.maxStageAttempts} times, and its shuffle input was lost each time: the query fails. Stop fewer nodes, or use more of them.`);
    }
    const w = s.stage.writes;
    const partitions = w ? this.tracker.missing(w.shuffle) : Array.from({ length: s.stage.partitions }, (_, p) => p).filter((p) => !s.results.has(p));
    s.attempt.set(this.seq, attempt);
    s.status.set(this.seq, "running");
    s.why.set(this.seq, `attempt ${attempt} running`);
    s.attempts.push({ attempt, seq: this.seq, at: this.now, partitions });
    s.start ??= this.now;
    this.scheduler.submit(id, attempt, s.stage.job, partitions);
    const kind = w ? "ShuffleMapStage" : "ResultStage";
    const parts = `${partitions.length} task${partitions.length === 1 ? "" : "s"}`;
    if (attempt === 0) {
      const after = s.stage.parents.length ? `: its parent stage${s.stage.parents.length === 1 ? "" : "s"} ${s.stage.parents.join(", ")} ${s.stage.parents.length === 1 ? "is" : "are"} done` : "";
      this.decide("StageSubmitted", `stage ${id} (${kind}, ${parts}) is submitted${after}`, { job: s.stage.job, stage: id });
    } else {
      this.decide("StageResubmitted", `stage ${id} (${kind}) is resubmitted as attempt ${attempt}, for ${parts}: partition${partitions.length === 1 ? "" : "s"} ${partitions.join(", ")}, whose output is missing`, { job: s.stage.job, stage: id });
    }
  }

  /** Gives pending tasks to free slots: one TaskStart step per task launched. */
  private launch(): void {
    if (this.failed || this.done) return;
    for (const l of this.scheduler.offer()) this.launched(l);
  }

  private launched(l: Launch): void {
    const st = this.stage(l.stage).stage;
    const t: TaskRecord = { ...l, seq: this.seq, at: this.now, status: new History<TaskStatus>(this.seq, "running"), end: null, error: null, fetchFailed: false, metrics: null };
    if (this.tasks.length !== l.tid) throw internal(`task ${l.tid} launched out of order`);
    this.tasks.push(t);
    const cmd: Command = { type: "LaunchTask", executor: l.executor, tid: l.tid, stage: l.stage, stageAttempt: l.stageAttempt, partition: l.partition, attempt: l.attempt, slot: l.slot, shuffles: this.tracker.inputs(st.reads), broadcasts: st.broadcasts };
    this.decide("TaskStart", `${taskName(t)} is launched on executor ${l.executor}, slot ${l.slot}`, { job: st.job, stage: l.stage, tid: l.tid, executor: l.executor, partition: l.partition }, [cmd]);
  }

  // --- events -----------------------------------------------------------------------

  on(e: DriverEvent, now: number): void {
    this.now = now;
    if (this.failed || this.done) {
      this.decide("TaskEnd", `${describeMessage(e)}: the query is over, so it is ignored`);
      return;
    }
    switch (e.type) {
      case "TaskFinished": return this.finished(e);
      case "TaskFailed": return this.taskFailed(e);
      case "ExecutorLost": return this.executorLost(e.executor);
      default: {
        const never: never = e;
        return never;
      }
    }
  }

  private finished(e: Extract<ExecutorEvent, { type: "TaskFinished" }>): void {
    const t = this.task(e.tid);
    this.scheduler.finished(e.tid);
    t.status.set(this.seq, "success");
    t.end = e.metrics.end;
    t.metrics = e.metrics;
    const s = this.stage(e.stage);
    const w = s.stage.writes;
    let what: string;
    if (w) {
      if (!e.mapStatus) throw internal(`map ${taskName(e)} sent no MapStatus`);
      this.tracker.register(w.shuffle, e.partition, e.mapStatus, e.tid, this.seq);
      what = `its MapStatus is registered: a shuffle file on node ${e.mapStatus.node}, ${e.mapStatus.bytes.reduce((a, b) => a + b, 0).toLocaleString("en")} bytes`;
    } else if (s.results.has(e.partition)) what = "partition already done: its rows are dropped";
    else {
      s.results.set(e.partition, e.rows ?? []);
      what = `${(e.rows?.length ?? 0).toLocaleString("en")} row(s) for the driver`;
    }
    this.decide("TaskEnd", `${taskName(e)} finished on executor ${e.executor}: ${what}`, { job: s.stage.job, stage: e.stage, tid: e.tid, executor: e.executor, partition: e.partition });
    this.afterTask(s);
  }

  /** A task of stage `s` ended: the stage may be complete, or need its missing
   * partitions again; free slots get tasks. */
  private afterTask(s: StageState): void {
    this.check(s);
    this.launch();
  }

  /** A running stage that is complete is done; one whose every task ended but that
   * misses outputs (lost with an executor since) runs its missing partitions again. */
  private check(s: StageState): void {
    if (s.status.now !== "running") return;
    const set = this.scheduler.setOf(s.stage.id, s.attempt.now);
    if (this.complete(s)) this.stageCompleted(s);
    else if (set && this.scheduler.over(set)) {
      s.status.set(this.seq, "waiting");
      this.submitStage(s.stage.id);
    }
  }

  private stageCompleted(s: StageState): void {
    const id = s.stage.id;
    s.status.set(this.seq, "done");
    s.why.set(this.seq, "done");
    s.end = this.now;
    this.scheduler.cancel(id);
    // Free what no stage reads any more.
    const up = this.scheduler.executorsUp();
    const commands: Command[] = [];
    const freed: string[] = [];
    for (const r of s.stage.reads) {
      if (this.plan.stages.filter((x) => x.reads.includes(r)).every((x) => this.stage(x.id).status.now === "done")) {
        this.tracker.remove(r, this.seq);
        for (const ex of up) commands.push({ type: "RemoveShuffle", executor: ex, shuffle: r });
        freed.push(`shuffle ${r}`);
      }
    }
    for (const b of s.stage.broadcasts) {
      if (this.plan.stages.filter((x) => x.broadcasts.includes(b)).every((x) => this.stage(x.id).status.now === "done")) {
        for (const ex of up) commands.push({ type: "RemoveBroadcast", executor: ex, broadcast: b });
        freed.push(`broadcast ${b}`);
      }
    }
    const out = s.stage.writes ? `its shuffle output (shuffle ${s.stage.writes.shuffle}) is available` : "every partition's rows are in";
    this.decide("StageCompleted", `stage ${id} is done: ${out}${freed.length ? `; nothing reads ${freed.join(" or ")} any more, so it is freed` : ""}`, { job: s.stage.job, stage: id }, commands);
    const job = this.plan.job(s.stage.job);
    if (job.stages.at(-1) === id) return this.jobEnd(job.id);
    for (const c of this.plan.children(id)) if (this.stage(c.id).status.now === "waiting") this.submitStage(c.id);
  }

  private jobEnd(id: number): void {
    const js = this.jobs[id]!; // ids come from the plan
    const job = this.plan.job(id);
    js.status.set(this.seq, "succeeded");
    js.end = this.now;
    const rows = this.stage(this.plan.resultStage(id).id);
    if (job.broadcast) {
      const all = [...rows.results.entries()].sort((a, b) => a[0] - b[0]).flatMap(([, r]) => [...r]);
      this.broadcastRows.set(job.broadcast.broadcast, all);
      this.decide("JobEnd", `job ${id} succeeded: broadcast ${job.broadcast.broadcast} has ${all.length.toLocaleString("en")} row(s), which executors fetch from the driver`, { job: id });
      this.startJobs();
      return;
    }
    this.decide("JobEnd", `job ${id} succeeded: the query's result is in`, { job: id });
    this.done = true;
    this.decide("SQLExecutionEnd", `the query is done: ${this.rows().length.toLocaleString("en")} row(s)`);
  }

  private taskFailed(e: Extract<ExecutorEvent, { type: "TaskFailed" }>): void {
    const t = this.task(e.tid);
    t.status.set(this.seq, "failed");
    t.end = e.metrics?.end ?? this.now;
    t.error = e.error;
    t.metrics = e.metrics;
    const s = this.stage(e.stage);
    const set = this.scheduler.setOf(e.stage, e.stageAttempt);
    const refs: Refs = { job: s.stage.job, stage: e.stage, tid: e.tid, executor: e.executor, partition: e.partition };
    const current = !!set && !set.zombie && s.attempt.now === e.stageAttempt && s.status.now === "running";
    if (e.fetchFailed) {
      t.fetchFailed = true;
      const ff = e.fetchFailed;
      if (!current || !set) {
        this.scheduler.failed(e.tid, false);
        this.decide("TaskEnd", `${taskName(e)} failed with FetchFailed, but stage ${e.stage}'s attempt ${e.stageAttempt} has failed already`, refs);
        return this.launch();
      }
      this.scheduler.makeZombie(set);
      this.scheduler.failed(e.tid, false);
      if (ff.node !== null && this.executors[ff.node]?.now === "gone") this.tracker.lostWith(ff.node, this.seq);
      const parent = this.plan.writer.get(ff.shuffle);
      if (parent === undefined) throw internal(`no stage writes shuffle ${ff.shuffle}`);
      const ps = this.stage(parent);
      if (ps.status.now === "done") {
        ps.status.set(this.seq, "waiting");
        ps.why.set(this.seq, "shuffle output lost");
      }
      s.status.set(this.seq, "waiting");
      s.why.set(this.seq, `attempt ${e.stageAttempt} failed: FetchFailed`);
      this.decide("FetchFailed", `${taskName(e)} could not read shuffle ${ff.shuffle} (${e.error}). Stage ${e.stage}'s attempt ${e.stageAttempt} fails; stage ${parent} is resubmitted for its missing partitions, and then stage ${e.stage}`, refs);
      this.submitStage(e.stage);
      return this.launch();
    }
    const n = this.scheduler.failed(e.tid, current);
    if (!current) {
      this.decide("TaskEnd", `${taskName(e)} failed (${e.error}), but its stage attempt is over: ignored`, refs);
      return this.launch();
    }
    if (n >= this.settings.maxTaskFailures) {
      return this.fail("task", `stage ${e.stage}`, `Task ${e.partition} in stage ${e.stage}.${e.stageAttempt} failed ${n} times; most recent failure: lost ${taskName(e)} on executor ${e.executor}: ${e.error}`);
    }
    this.decide("TaskEnd", `${taskName(e)} failed on executor ${e.executor} (failure ${n} of ${this.settings.maxTaskFailures}): ${e.error}; partition ${e.partition} runs again`, refs);
    this.afterTask(s);
  }

  private executorLost(executor: number): void {
    const ex = this.executors[executor];
    if (!ex) throw internal(`there is no executor ${executor}`);
    ex.set(this.seq, "gone");
    const lostTasks = this.scheduler.executorLost(executor);
    for (const tid of lostTasks) {
      const t = this.task(tid);
      t.status.set(this.seq, "lost");
      t.end = this.now;
      t.error = `executor ${executor} is gone`;
    }
    const lost = this.tracker.lostWith(executor, this.seq);
    const byStage = new Map<number, number>();
    for (const o of lost) {
      const p = this.plan.writer.get(o.shuffle);
      if (p === undefined) throw internal(`no stage writes shuffle ${o.shuffle}`);
      byStage.set(p, (byStage.get(p) ?? 0) + 1);
      const ps = this.stage(p);
      if (ps.status.now === "done") {
        ps.status.set(this.seq, "waiting");
        ps.why.set(this.seq, `shuffle output lost with executor ${executor}`);
      }
    }
    const what = [
      lostTasks.length ? `its ${lostTasks.length} running task${lostTasks.length === 1 ? "" : "s"} will run again elsewhere` : "",
      ...[...byStage].map(([st, n]) => `${n} shuffle output${n === 1 ? "" : "s"} of stage ${st} ${n === 1 ? "is" : "are"} lost`),
    ].filter(Boolean);
    this.decide("ExecutorLost", `executor ${executor} is gone, with its node${what.length ? `: ${what.join("; ")}` : ""}`, { executor });
    if (!this.scheduler.executorsUp().length) {
      const waiting = this.stages.find((s) => s.status.now !== "done" && this.jobs[s.stage.job]?.status.now === "running") ?? this.stages.find((s) => s.status.now !== "done");
      const id = waiting?.stage.id ?? 0;
      return this.fail("executors", `stage ${id}`, `every node is gone: no executor can run stage ${id}`);
    }
    // A running stage whose every task has ended may now miss outputs.
    for (const s of this.stages) this.check(s);
    this.launch();
  }

  private fail(cause: "task" | "stage" | "executors", unit: string, error: string): void {
    this.failed = { unit, error, seq: this.seq, cause };
    this.scheduler.stopAll();
    for (const j of this.jobs) if (j.status.now === "running") j.status.set(this.seq, "failed");
    this.decide("SQLExecutionFailed", `the query failed in ${unit}: ${error}`);
  }
}
