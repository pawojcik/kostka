// Aggregate state in two phases, for every engine's aggregates. Each aggregate of a
// query has a state (core/functions/aggregates.ts: a fixed list of typed values with
// init, add, merge and result). A group is its key and one state per aggregate.
//
// - Partial: input rows are folded into groups (`add`); a group is written out as
//   its key followed by every aggregate's state, flat (`partial`), so it can travel
//   between nodes like any other row.
// - Final: partial rows of one group are merged (`merge`), and the group's result is
//   its key followed by each aggregate's result (`result`).
//
// AggLayout says where each aggregate's state sits in a partial row.

import type { Evaluator } from "./compile.ts";
import type { AggCall } from "./expr.ts";
import type { BoundAggregate } from "./functions/index.ts";
import type { Row, Value } from "./types.ts";

/** A group: its key values and one state per aggregate. */
export interface Group {
  key: Value[];
  states: Value[][];
}

/** The aggregate implementations and where each one's state sits in a partial row. */
export class AggLayout {
  readonly impls: BoundAggregate[];
  readonly offsets: number[];
  readonly width: number;
  constructor(calls: readonly AggCall[]) {
    this.impls = calls.map((c) => c.bound);
    this.offsets = [];
    let o = 0;
    for (const impl of this.impls) {
      this.offsets.push(o);
      o += impl.state.length;
    }
    this.width = o;
  }

  /** A new group's states. */
  init(): Value[][] {
    return this.impls.map((i) => i.init());
  }

  /** Folds one input row into a group's states. `args` evaluates each aggregate's
   * argument (null: count(*), which counts every row); a null argument is skipped. */
  add(states: Value[][], args: readonly (Evaluator | null)[], row: Row): void {
    const impls = this.impls;
    for (let i = 0; i < impls.length; i++) {
      const a = args[i];
      const v = a ? a(row) : true;
      if (v !== null) impls[i]!.add(states[i]!, v);
    }
  }

  /** Merges a partial row (its `nKeys` key values, then the states) into states. */
  merge(states: Value[][], row: Row, nKeys: number): void {
    const impls = this.impls;
    for (let a = 0; a < impls.length; a++) {
      const o = nKeys + this.offsets[a]!;
      impls[a]!.merge(states[a]!, row.slice(o, o + impls[a]!.state.length));
    }
  }

  /** A group as a partial row: its key, then every state, flat. */
  partial(g: Group): Row {
    return [...g.key, ...g.states.flat()];
  }

  /** A group's final row: its key, then each aggregate's result. */
  result(g: Group): Row {
    return [...g.key, ...g.states.map((st, a) => this.impls[a]!.result(st))];
  }
}
