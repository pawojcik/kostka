import { formatValue, type DataType, type Value } from "../../core/types.ts";

export const n = (x: number | null | undefined): string => (x === null || x === undefined ? "—" : x.toLocaleString("en-US"));

export function ms(x: number | null | undefined): string {
  if (x === null || x === undefined) return "—";
  if (x === 0) return "0 ms";
  if (x < 0.01) return `${(x * 1000).toFixed(1)} µs`;
  if (x < 10) return `${x.toFixed(2)} ms`;
  if (x < 1000) return `${x.toFixed(0)} ms`;
  return `${(x / 1000).toFixed(2)} s`;
}

export function bytes(b: number | null | undefined): string {
  if (b === null || b === undefined) return "—";
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / 1024 / 1024).toFixed(1)} MB`;
}

export function value(v: Value, t: string): string {
  if (v === null) return "null";
  // floats to 15 significant digits: 3.51, not the sum's 3.5100000000000002
  if (t === "float" && typeof v === "number" && Number.isFinite(v)) return String(Number(v.toPrecision(15)));
  return formatValue(v, t as DataType);
}

/** The class of a pipeline's color: P0–P7 take the eight slots, later ones are gray. */
/** A pipeline's color class: 20 colors, then gray. */
export const PIPELINE_COLORS = 20;
export const pc = (i: number): string => (i >= 0 && i < PIPELINE_COLORS ? `pc-${i}` : "pc-x");

export const CATS: [string, string][] = [["cpu", "processing"], ["storage", "storage read"], ["network", "network"], ["spill_write", "spill write"], ["spill_read", "spill read"]];
