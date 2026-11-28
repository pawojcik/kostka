// The MapOutputTracker: where each map task's shuffle file is. For every shuffle it
// keeps one entry per map partition: the MapStatus a finished map task reported (its
// node, block and bytes per reduce partition), or nothing yet. Reduce tasks are
// launched with these locations. When a node is gone its entries are lost, and when
// no stage reads a shuffle any more its entries are removed; either way the map
// partition is missing again, and its stage must run it again before a reader can.
//
// Each change remembers the step it happened at, for the workbench.

import { History } from "../../../core/history.ts";
import { internal } from "../../../core/errors.ts";
import type { MapStatus, ShuffleInput } from "../protocol.ts";

export type OutputStatus = "available" | "lost" | "removed";

export interface MapOutput {
  readonly status: MapStatus;
  readonly state: OutputStatus;
  /** The task attempt that wrote it. */
  readonly tid: number;
}

export class MapOutputTracker {
  /** Shuffle -> map partition -> its output, step by step (undefined: never written). */
  private readonly outputs: History<MapOutput | null>[][];

  /** `maps[s]`: the map partitions of shuffle s. */
  constructor(maps: readonly number[]) {
    this.outputs = maps.map((n) => Array.from({ length: n }, () => new History<MapOutput | null>(0, null)));
  }

  private entry(shuffle: number, map: number): History<MapOutput | null> {
    const e = this.outputs[shuffle]?.[map];
    if (!e) throw internal(`shuffle ${shuffle} has no map partition ${map}`);
    return e;
  }

  register(shuffle: number, map: number, status: MapStatus, tid: number, seq: number): void {
    this.entry(shuffle, map).set(seq, { status, state: "available", tid });
  }

  /** The map partitions of a shuffle with no available output. */
  missing(shuffle: number): number[] {
    const list = this.outputs[shuffle];
    if (!list) throw internal(`there is no shuffle ${shuffle}`);
    return list.flatMap((h, i) => (h.now?.state === "available" ? [] : [i]));
  }

  available(shuffle: number): boolean {
    return this.missing(shuffle).length === 0;
  }

  /** Every output on `node` is lost; returns the (shuffle, map) pairs that were. */
  lostWith(node: number, seq: number): { shuffle: number; map: number }[] {
    const lost: { shuffle: number; map: number }[] = [];
    this.outputs.forEach((list, shuffle) => list.forEach((h, map) => {
      const o = h.now;
      if (o && o.state === "available" && o.status.node === node) {
        h.set(seq, { ...o, state: "lost" });
        lost.push({ shuffle, map });
      }
    }));
    return lost;
  }

  /** No stage reads `shuffle` any more: its outputs are removed. */
  remove(shuffle: number, seq: number): void {
    for (const h of this.outputs[shuffle] ?? []) {
      const o = h.now;
      if (o && o.state === "available") h.set(seq, { ...o, state: "removed" });
    }
  }

  /** What a reduce task of these shuffles is told: each map output's location, or null. */
  inputs(shuffles: readonly number[]): ShuffleInput[] {
    return shuffles.map((shuffle) => ({
      shuffle,
      maps: (this.outputs[shuffle] ?? []).map((h) => (h.now?.state === "available" ? h.now.status : null)),
    }));
  }

  /** The outputs of a shuffle right after step `at`, for the workbench. */
  at(shuffle: number, at: number): (MapOutput | null)[] {
    return (this.outputs[shuffle] ?? []).map((h) => h.after(at) ?? null);
  }
}
