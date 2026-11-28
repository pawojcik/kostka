// An executor's block manager: the shuffle files its tasks wrote (on its node's
// disk), and the broadcast values it fetched from the driver (in its node's memory).
// A shuffle file stays until the driver says no stage reads it (RemoveShuffle); a
// broadcast value is built once per executor and shared by every task on it, until
// RemoveBroadcast. Everything here is gone with the node.

/** A shuffle file this executor holds. */
export interface ShuffleFile {
  readonly shuffle: number;
  readonly map: number;
  readonly tid: number;
  readonly block: number;
  readonly bytes: number;
  /** Its key in the node's memory (on the disk side). */
  readonly holdKey: string;
}

/** A broadcast value this executor built, for one operator that reads it. */
export interface BroadcastValue {
  readonly broadcast: number;
  readonly op: number;
  readonly value: unknown;
  readonly bytes: number;
  readonly holdKey: string;
  hits: number;
}

export class BlockManager {
  private readonly files: ShuffleFile[] = [];
  private readonly values = new Map<string, BroadcastValue>();

  addFile(f: ShuffleFile): void {
    this.files.push(f);
  }

  /** Drops this executor's files of `shuffle`; returns them, to free their blocks. */
  removeShuffle(shuffle: number): ShuffleFile[] {
    const out = this.files.filter((f) => f.shuffle === shuffle);
    for (const f of out) this.files.splice(this.files.indexOf(f), 1);
    return out;
  }

  /** Drops a file a failed task wrote. */
  dropFile(f: ShuffleFile): void {
    const i = this.files.indexOf(f);
    if (i >= 0) this.files.splice(i, 1);
  }

  value(broadcast: number, op: number): BroadcastValue | undefined {
    return this.values.get(`${broadcast}:${op}`);
  }

  addValue(v: BroadcastValue): void {
    this.values.set(`${v.broadcast}:${v.op}`, v);
  }

  /** Drops this executor's values of `broadcast`; returns them, to free their memory. */
  removeBroadcast(broadcast: number): BroadcastValue[] {
    const out = [...this.values.values()].filter((v) => v.broadcast === broadcast);
    for (const v of out) this.values.delete(`${v.broadcast}:${v.op}`);
    return out;
  }

  /** What it holds, for the workbench. */
  state(): { shuffle_files: { shuffle: number; map: number; tid: number; bytes: number }[]; broadcasts: { broadcast: number; op: number; bytes: number; hits: number }[] } {
    return {
      shuffle_files: this.files.map((f) => ({ shuffle: f.shuffle, map: f.map, tid: f.tid, bytes: f.bytes })),
      broadcasts: [...this.values.values()].map((v) => ({ broadcast: v.broadcast, op: v.op, bytes: v.bytes, hits: v.hits })),
    };
  }
}
