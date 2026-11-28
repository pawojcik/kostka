// Blocks: rows a node keeps for others to read, and the data plane that reads them.
// Infrastructure. A block is what an engine's output becomes on a node (kostka's
// pieces of output buffers; Spark's shuffle files): an id, its rows, their size, and
// whether it was spilled to the node's disk (its memory decides that; memory.ts).
//
// The data plane reads a block from the node holding it. If that node is gone, the
// block is gone with it: reading it throws BlockLostError, and the engine decides
// what to do (run the reader again once the block is made again, say).

import { internal } from "../core/errors.ts";
import type { Row } from "../core/types.ts";
import { rowsBytes } from "../core/rows.ts";

export interface Block {
  /** Unique on its node. */
  readonly id: number;
  readonly node: number;
  readonly rows: readonly Row[];
  readonly bytes: number;
  /** On the node's disk rather than in its memory: reading it costs disk time. */
  spilled: boolean;
}

/** The blocks one node holds. */
export class BlockStore {
  readonly node: number;
  private readonly blocks = new Map<number, Block>();
  private next = 0;

  constructor(node: number) {
    this.node = node;
  }

  /** Keeps `rows` as a new block. */
  put(rows: readonly Row[]): Block {
    const block: Block = { id: this.next++, node: this.node, rows, bytes: rowsBytes(rows), spilled: false };
    this.blocks.set(block.id, block);
    return block;
  }

  get(id: number): Block | undefined {
    return this.blocks.get(id);
  }

  drop(id: number): void {
    this.blocks.delete(id);
  }

  get size(): number {
    return this.blocks.size;
  }
}

/** A block's node is gone, and the block with it. */
export class BlockLostError extends Error {
  readonly node: number;
  readonly block: number;
  constructor(node: number, block: number) {
    super(`block ${block} is gone with node ${node}: make it again on a node that is up`);
    this.name = "BlockLostError";
    this.node = node;
    this.block = block;
  }
}

/** Reads blocks from the nodes holding them. */
export class DataPlane {
  private readonly nodes: readonly { readonly up: boolean; readonly blocks: BlockStore }[];

  constructor(nodes: readonly { readonly up: boolean; readonly blocks: BlockStore }[]) {
    this.nodes = nodes;
  }

  /** Block `id` of node `node`, or undefined if the node dropped it (its engine freed
   * it). Throws BlockLostError if that node is gone. */
  lookup(node: number, id: number): Block | undefined {
    const holder = this.nodes[node];
    if (!holder) throw internal(`there is no node ${node}`);
    if (!holder.up) throw new BlockLostError(node, id);
    return holder.blocks.get(id);
  }

  /** Block `id` of node `node`. Throws BlockLostError if that node is gone. */
  read(node: number, id: number): Block {
    const holder = this.nodes[node];
    if (!holder) throw internal(`there is no node ${node}`);
    if (!holder.up) throw new BlockLostError(node, id);
    const block = holder.blocks.get(id);
    if (!block) throw internal(`node ${node} does not hold block ${id}: it was dropped while a reader still needed it`);
    return block;
  }
}
