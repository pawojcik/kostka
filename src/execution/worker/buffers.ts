// What one worker holds of the output buffers: the pieces the splits it ran wrote,
// with their rows, until the coordinator says every reader of a buffer is done
// (ReleaseBuffer). Other workers read them through the data plane, by PieceRef.

import type { Row } from "../../core/types.ts";
import type { PieceRef } from "../../physical/buffer.ts";
import { rowsBytes } from "../../physical/metrics.ts";

export class LocalPiece {
  readonly ref: PieceRef;
  readonly rows: Row[];
  spilled = false;
  constructor(ref: PieceRef, rows: Row[]) {
    this.ref = ref;
    this.rows = rows;
  }
}

export class LocalBuffers {
  readonly worker: number;
  /** buffer -> its pieces on this worker, in the order written. */
  private readonly pieces = new Map<number, LocalPiece[]>();
  private readonly byId = new Map<number, LocalPiece>();
  private next = 0;

  constructor(worker: number) {
    this.worker = worker;
  }

  add(buffer: number, partition: number, split: number, rows: Row[]): LocalPiece {
    const ref: PieceRef = { id: this.next++, buffer, partition, worker: this.worker, split, rows: rows.length, bytes: rowsBytes(rows) };
    const piece = new LocalPiece(ref, rows);
    const list = this.pieces.get(buffer) ?? [];
    list.push(piece);
    this.pieces.set(buffer, list);
    this.byId.set(ref.id, piece);
    return piece;
  }

  get(id: number): LocalPiece | undefined {
    return this.byId.get(id);
  }

  /** Drops pieces a failed split wrote. */
  drop(pieces: readonly LocalPiece[]): void {
    for (const p of pieces) {
      this.byId.delete(p.ref.id);
      const list = this.pieces.get(p.ref.buffer);
      if (list) this.pieces.set(p.ref.buffer, list.filter((x) => x !== p));
    }
  }

  /** Pieces held, per buffer. */
  counts(): { buffer: number; pieces: number; rows: number; bytes: number; spilled: number }[] {
    return [...this.pieces].map(([buffer, list]) => ({
      buffer, pieces: list.length, rows: list.reduce((a, p) => a + p.ref.rows, 0), bytes: list.reduce((a, p) => a + p.ref.bytes, 0), spilled: list.filter((p) => p.spilled).length,
    }));
  }

  /** Drops a buffer's pieces; returns them (to free their memory). */
  release(buffer: number): LocalPiece[] {
    const list = this.pieces.get(buffer) ?? [];
    this.pieces.delete(buffer);
    for (const p of list) this.byId.delete(p.ref.id);
    return list;
  }
}

export const holdKeyOf = (ref: PieceRef) => `piece:B${ref.buffer}:${ref.id}`;
