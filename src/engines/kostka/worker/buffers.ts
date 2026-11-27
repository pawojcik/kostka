// What one worker holds of the output buffers: the pieces the splits it ran wrote,
// until the coordinator says every reader of a buffer is done (ReleaseBuffer). A
// piece's rows are a block in its node's block store (cluster/blocks.ts); this keeps
// what kostka knows of each (its buffer, partition and split). Other workers read
// pieces through the data plane, by PieceRef.

import type { PieceRef } from "../physical/buffer.ts";
import type { PieceBlock } from "./worker.ts";

/** A piece: a block of the node, as one output's share of a buffer. */
export interface LocalPiece {
  readonly ref: PieceRef;
  readonly block: PieceBlock;
}

export class LocalBuffers {
  readonly worker: number;
  /** buffer -> its pieces on this worker, in the order written. */
  private readonly pieces = new Map<number, LocalPiece[]>();

  constructor(worker: number) {
    this.worker = worker;
  }

  /** Records `block` as a piece of `partition` of `buffer`, written by `split`. */
  add(buffer: number, partition: number, split: number, block: PieceBlock): LocalPiece {
    const ref: PieceRef = { id: block.id, buffer, partition, worker: this.worker, split, rows: block.rows.length, bytes: block.bytes };
    const piece: LocalPiece = { ref, block };
    const list = this.pieces.get(buffer) ?? [];
    list.push(piece);
    this.pieces.set(buffer, list);
    return piece;
  }

  /** Forgets pieces a failed split wrote. */
  drop(pieces: readonly LocalPiece[]): void {
    for (const p of pieces) {
      const list = this.pieces.get(p.ref.buffer);
      if (list) this.pieces.set(p.ref.buffer, list.filter((x) => x !== p));
    }
  }

  /** Pieces held, per buffer. */
  counts(): { buffer: number; pieces: number; rows: number; bytes: number; spilled: number }[] {
    return [...this.pieces].map(([buffer, list]) => ({
      buffer, pieces: list.length, rows: list.reduce((a, p) => a + p.ref.rows, 0), bytes: list.reduce((a, p) => a + p.ref.bytes, 0), spilled: list.filter((p) => p.block.spilled).length,
    }));
  }

  /** Forgets a buffer's pieces; returns them (to free their memory and blocks). */
  release(buffer: number): LocalPiece[] {
    const list = this.pieces.get(buffer) ?? [];
    this.pieces.delete(buffer);
    return list;
  }
}

export const holdKeyOf = (ref: PieceRef) => `piece:B${ref.buffer}:${ref.id}`;
