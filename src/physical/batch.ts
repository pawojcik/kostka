// A batch: rows with their column list. The columns are Fields with expression ids,
// so an operator can check that it received the layout it was compiled for.

import { internal } from "../core/errors.ts";
import type { Field } from "../core/expr.ts";
import type { Row } from "../core/types.ts";

export class Batch {
  readonly columns: readonly Field[];
  readonly rows: Row[];
  constructor(columns: readonly Field[], rows: Row[]) {
    this.columns = columns;
    this.rows = rows;
  }
  get length(): number {
    return this.rows.length;
  }
  toString(): string {
    return `Batch([${this.columns.map((f) => f.label()).join(", ")}], ${this.rows.length} rows)`;
  }
}

export function checkColumns(batch: Batch, columns: readonly Field[], who: string): void {
  if (batch.columns === columns) return;
  const same = batch.columns.length === columns.length && batch.columns.every((f, i) => f.id === columns[i]!.id);
  if (!same) throw internal(`${who} expected columns [${columns.map((f) => f.label()).join(", ")}], got ${batch}`);
}
