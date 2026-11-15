// Aggregate functions. An aggregate runs in two phases (see the physical planner):
// each split folds its rows into a *state* per group, the states are shuffled by group
// key, and a later split merges the states of a group and computes the result. So an
// aggregate is defined by its state: a fixed list of typed values, which travels in
// the output buffer's rows like any other column.
//
// Adding one means registering an AggregateFunction whose `resolve` returns a
// BoundAggregate for the argument type, e.g. a `bool_and`:
//
//   registerAggregate({ name: "bool_and", doc: "...", resolve: (t) => ({
//     type: "bool", state: ["bool"], init: () => [null],
//     add: (s, v) => { s[0] = s[0] === null ? v : s[0] && v; },
//     merge: (s, o) => { if (o[0] !== null) s[0] = s[0] === null ? o[0] : s[0] && o[0]; },
//     result: (s) => s[0] }) });

import { ExecutionError } from "../errors.ts";
import { compareValues, isNumeric, type DataType, type Value } from "../types.ts";
import { TypeMismatch } from "./registry.ts";

/** An aggregate bound to its argument type: its result type and state machine. */
export interface BoundAggregate {
  readonly type: DataType;
  /** The types of the state's values. */
  readonly state: readonly DataType[];
  init(): Value[];
  /** Folds one non-null input value in (count(*) gets `true` for every row). */
  add(state: Value[], v: Value): void;
  /** Folds another state of the same group in. */
  merge(state: Value[], other: readonly Value[]): void;
  result(state: readonly Value[]): Value;
}

export interface AggregateFunction {
  readonly name: string;
  readonly doc: string;
  readonly signature: string;
  /** Binds it to the argument type (null for `count(*)`); throws TypeMismatch if it
   * takes no such argument. */
  resolve(argType: DataType | null): BoundAggregate;
}

const aggregates = new Map<string, AggregateFunction>();

export function registerAggregate(fn: AggregateFunction): AggregateFunction {
  aggregates.set(fn.name, fn);
  return fn;
}
export const lookupAggregate = (name: string) => aggregates.get(name);
export const allAggregates = () => [...aggregates.values()];

function addNumbers(a: number, b: number, int: boolean): number {
  const r = a + b;
  if (int ? !Number.isSafeInteger(r) : !Number.isFinite(r)) {
    throw new ExecutionError(`sum() overflowed${int ? " the int range (±2^53)" : ""}`);
  }
  return r;
}

const count: BoundAggregate = {
  type: "int", state: ["int"], init: () => [0],
  add: (s) => { s[0] = (s[0] as number) + 1; },
  merge: (s, o) => { s[0] = (s[0] as number) + (o[0] as number); },
  result: (s) => s[0]!,
};
registerAggregate({
  name: "count", doc: "the number of rows (count(*)) or of non-null values", signature: "count(* | x) → int",
  resolve: () => count,
});

function numeric(name: string, t: DataType | null): DataType {
  if (t === null) throw new TypeMismatch(`${name}(*) is not an aggregate; use count(*)`);
  if (!isNumeric(t) && t !== "null") throw new TypeMismatch(`${name}() needs a number, got ${t}`);
  return t === "null" ? "float" : t;
}

registerAggregate({
  name: "sum", doc: "the sum of the non-null values; NULL if there are none", signature: "sum(x int | float) → x's type",
  resolve: (t) => {
    const type = numeric("sum", t), int = type === "int";
    return {
      type, state: [type], init: () => [null],
      add: (s, v) => { s[0] = s[0] === null ? v : addNumbers(s[0] as number, v as number, int); },
      merge: (s, o) => { if (o[0] !== null) s[0] = s[0] === null ? o[0]! : addNumbers(s[0] as number, o[0] as number, int); },
      result: (s) => s[0]!,
    };
  },
});
registerAggregate({
  name: "avg", doc: "the mean of the non-null values, as a float", signature: "avg(x int | float) → float",
  resolve: (t) => {
    numeric("avg", t);
    return {
      type: "float", state: ["int", "float"], init: () => [0, 0],
      add: (s, v) => { s[0] = (s[0] as number) + 1; s[1] = addNumbers(s[1] as number, v as number, false); },
      merge: (s, o) => { s[0] = (s[0] as number) + (o[0] as number); s[1] = addNumbers(s[1] as number, o[1] as number, false); },
      result: (s) => ((s[0] as number) === 0 ? null : (s[1] as number) / (s[0] as number)),
    };
  },
});
for (const [name, better] of [["min", -1], ["max", 1]] as const) {
  registerAggregate({
    name, doc: `the ${name === "min" ? "smallest" : "largest"} non-null value`, signature: `${name}(x) → x's type`,
    resolve: (t) => {
      if (t === null) throw new TypeMismatch(`${name}(*) is not an aggregate; use count(*)`);
      const keep = (s: Value[], v: Value) => {
        if (v !== null && (s[0] === null || Math.sign(compareValues(v, s[0]!)) === better)) s[0] = v;
      };
      return { type: t, state: [t], init: () => [null], add: keep, merge: (s, o) => keep(s, o[0]!), result: (s) => s[0]! };
    },
  });
}
