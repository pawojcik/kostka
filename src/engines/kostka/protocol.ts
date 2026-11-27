// The messages between the coordinator and the workers: the only way the two talk.
// Commands go from the coordinator to one worker; events come to the coordinator,
// from a worker or (WorkerGone) from the infrastructure. They carry descriptions
// (splits, PieceRefs, metrics, filter keys), never shared objects; only SplitDone
// carries rows, and only the result's.
//
// A real deployment would serialize them; the simulation (sim/) passes them as they
// are, in simulated time.

import type { Row } from "../../core/types.ts";
import type { PieceRef, Split } from "./physical/buffer.ts";
import type { Category } from "./physical/context.ts";
import type { FilterKeys } from "./physical/operators/runtime-filter.ts";

/** A buffer a split reads whole (a build side): the pieces of one partition. */
export interface WholeInputPieces {
  readonly buffer: number;
  readonly partition: number;
  readonly pieces: readonly PieceRef[];
}

/** A split as the coordinator sends it: with where the data it reads is now. */
export interface Assigned {
  readonly split: Split;
  /** 1, or more when it runs again (after a failure). */
  readonly attempt: number;
  /** The pieces of the outputs a buffer split reads. */
  readonly pieces: readonly PieceRef[];
  readonly inputs: readonly WholeInputPieces[];
}

export type Command =
  | {
      readonly type: "AddSplits"; readonly worker: number; readonly pipeline: number;
      readonly splits: readonly Assigned[];
      /** The pipeline's runtime filter keys, the first time this worker gets its splits. */
      readonly filters: readonly FilterKeys[];
    }
  /** Every reader of a buffer is done: free its pieces and what was built from it. */
  | { readonly type: "ReleaseBuffer"; readonly worker: number; readonly buffer: number };

/** One operator's work in a split, and its instance's state. */
export interface OpReport {
  readonly rowsIn: number;
  readonly rowsOut: number;
  readonly batchesIn: number;
  readonly batchesOut: number;
  readonly bytesIn: number;
  readonly bytesOut: number;
  readonly time: Record<Category, number>;
  readonly notes: readonly string[];
  readonly state: Record<string, unknown>;
}

/** Where a split's time went, on its worker's timeline. */
export interface UnitReport {
  readonly start: number;
  readonly end: number;
  readonly time: Record<Category, number>;
  /** [kind, start, end]: a category, or "wait" (for the node's IO channel). */
  readonly segments: readonly [Category | "wait", number, number][];
  readonly ops: readonly OpReport[];
  /** Hash tables it built (and cached), described. */
  readonly tables: readonly string[];
}

export type WorkerEvent =
  | {
      readonly type: "SplitDone"; readonly worker: number; readonly pipeline: number; readonly split: number; readonly attempt: number;
      /** Every piece it wrote: its outputs. */
      readonly pieces: readonly PieceRef[];
      readonly filters: readonly FilterKeys[];
      /** Rows of the result (the last pipeline), else null. */
      readonly result: readonly Row[] | null;
      readonly unit: UnitReport;
    }
  | {
      readonly type: "SplitFailed"; readonly worker: number; readonly pipeline: number; readonly split: number; readonly attempt: number;
      readonly error: string; readonly name: string;
      /** A piece it read was gone with its worker: it can run again later. */
      readonly lost: { readonly worker: number; readonly piece: number } | null;
    };

/** From the infrastructure: a worker stopped, with everything it held. */
export interface WorkerGone {
  readonly type: "WorkerGone";
  readonly worker: number;
}

export type CoordinatorEvent = WorkerEvent | WorkerGone;
export type Message = Command | CoordinatorEvent;

/** A piece's worker is gone (its node is): the split reading it can run again once
 * the piece is made again. The worker's view of the cluster's BlockLostError. */
export class PieceLostError extends Error {
  readonly worker: number;
  readonly piece: number;
  constructor(ref: PieceRef) {
    super(`piece ${ref.id} of B${ref.buffer} is gone with W${ref.worker}`);
    this.name = "PieceLostError";
    this.worker = ref.worker;
    this.piece = ref.id;
  }
}

/** One line about a message, for the log. */
export function describeMessage(m: Message): string {
  const n = (x: number) => x.toLocaleString("en");
  const pl = (x: number, w: string) => `${n(x)} ${w}${x === 1 ? "" : "s"}`;
  const att = (a: number) => (a > 1 ? ` (attempt ${a})` : "");
  switch (m.type) {
    case "AddSplits": {
      const pieces = m.splits.reduce((a, s) => a + s.pieces.length, 0);
      const inputs = m.splits.reduce((a, s) => a + s.inputs.reduce((b, i) => b + i.pieces.length, 0), 0);
      const extra = [pieces ? `the locations of ${pl(pieces, "piece")}` : "", inputs ? `${pl(inputs, "build piece")}` : "", m.filters.length ? `keys of ${pl(m.filters.length, "runtime filter")}` : ""].filter(Boolean);
      return `AddSplits P${m.pipeline}: ${m.splits.map((s) => `split ${s.split.id}${att(s.attempt)}`).join(", ")}${extra.length ? `, with ${extra.join(", ")}` : ""}`;
    }
    case "ReleaseBuffer": return `ReleaseBuffer B${m.buffer}`;
    case "SplitDone": {
      const parts = new Set(m.pieces.map((p) => `${p.buffer}:${p.partition}`)).size;
      return `SplitDone: split ${m.split}${att(m.attempt)}${m.pieces.length ? `, ${pl(m.pieces.length, "piece")} in ${pl(parts, "output")}` : ""}${m.result ? `, ${pl(m.result.length, "result row")}` : ""}${m.filters.length ? `, keys for ${pl(m.filters.length, "runtime filter")}` : ""}`;
    }
    case "SplitFailed": return `SplitFailed: split ${m.split}${att(m.attempt)}: ${m.lost ? `a piece it read was on W${m.lost.worker}, which is gone` : m.error}`;
    case "WorkerGone": return `WorkerGone: W${m.worker}`;
  }
}
