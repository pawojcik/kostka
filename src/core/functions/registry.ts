// The function registry: every scalar function and operator is a `ScalarFunction`,
// looked up by name. Adding one is a matter of registering it (see scalar.ts):
//
//   register({
//     name: "reverse", category: "string", doc: "the string backwards",
//     resolve: overloads([{ args: ["string"], returns: "string",
//                           impl: (s) => [...(s as string)].reverse().join("") }]),
//   });
//
// Two interfaces:
//
// - ScalarFunction: the function as registered, looked up by name. `resolve` runs at
//   analysis time with the argument types and binds it to them, or throws a
//   TypeMismatch that the analyzer reports with the call's position.
// - BoundFunction: the function for those argument types, which a Call keeps: its
//   result type, the types its arguments are cast to, and `compile`, which builds the
//   call's evaluator from its arguments' evaluators.
//
// Most functions are null-propagating: an `impl` that only sees non-null arguments,
// where a null argument makes the result null; `strict(impl)` turns one into a
// `compile`. A function that treats nulls itself (and, or, coalesce, IS NULL) or
// evaluates lazily writes `compile` directly. `overloads` builds `resolve` from a
// table of signatures, each with an `impl` or a `compile`.

import { Call, Cast, Literal, type Expr } from "../expr.ts";
import { internal } from "../errors.ts";
import { type DataType, type Row, type Value } from "../types.ts";

export type Evaluator = (row: Row) => Value;
export type Impl = (...args: Value[]) => Value;

/** A function bound to the types of its arguments. */
export interface BoundFunction {
  /** The result type. */
  readonly type: DataType;
  /** The types the arguments are cast to first (implicit widening). */
  readonly argTypes: readonly DataType[];
  /** The call's evaluator, from its arguments' evaluators. */
  compile(args: readonly Evaluator[]): Evaluator;
}

export type Category = "arithmetic" | "comparison" | "logic" | "string" | "date" | "math" | "conditional";

export interface ScalarFunction {
  readonly name: string;
  readonly category: Category;
  readonly doc: string;
  /** e.g. "substring(s string, start int [, length int]) → string", for autocomplete. */
  readonly signature?: string;
  /** Operators are not offered as functions in autocomplete. */
  readonly operator?: boolean;
  /** Binds the function to these argument types; throws TypeMismatch if it takes
   * no such arguments. */
  resolve(argTypes: readonly DataType[]): BoundFunction;
  /** How a call is printed (operators: infix); default `name(a, b)`. */
  format?(args: readonly string[]): string;
}

/** Arguments of the wrong number or types. The analyzer adds the call's position. */
export class TypeMismatch extends Error {
  override name = "TypeMismatch";
}

const functions = new Map<string, ScalarFunction>();

export function register(fn: ScalarFunction): ScalarFunction {
  functions.set(fn.name, fn);
  return fn;
}

export function lookupFunction(name: string): ScalarFunction | undefined {
  return functions.get(name);
}

export function allFunctions(): ScalarFunction[] {
  return [...functions.values()];
}

// --- overload resolution --------------------------------------------------------

/** The implicit widenings: an int is usable as a float, a date as a timestamp (its
 * midnight), and NULL as anything. Nothing else converts without a CAST. */
export function widensTo(from: DataType, to: DataType): boolean {
  return from === to || from === "null" || (from === "int" && to === "float") || (from === "date" && to === "timestamp");
}

/** The common type of several (CASE branches, IN lists, coalesce), or null. */
export function commonType(types: readonly DataType[]): DataType | null {
  let out: DataType = "null";
  for (const t of types) {
    if (widensTo(t, out)) continue;
    if (widensTo(out, t)) out = t;
    else return null;
  }
  return out;
}

export interface Overload {
  readonly args: readonly DataType[];
  /** The type of any further arguments (variadic functions). */
  readonly rest?: DataType;
  readonly returns: DataType;
  readonly impl?: Impl;
  readonly compile?: (args: readonly Evaluator[]) => Evaluator;
}

/** The evaluator of a null-propagating function: a null argument makes the result
 * null, and `impl` sees non-null arguments only. */
export function strict(impl: Impl): (args: readonly Evaluator[]) => Evaluator {
  return (args) => {
    switch (args.length) {
      case 0: return () => impl();
      case 1: {
        const [a] = args as [Evaluator];
        return (row) => {
          const x = a(row);
          return x === null ? null : impl(x);
        };
      }
      case 2: {
        const [a, b] = args as [Evaluator, Evaluator];
        return (row) => {
          const x = a(row);
          if (x === null) return null;
          const y = b(row);
          return y === null ? null : impl(x, y);
        };
      }
      default:
        return (row) => {
          const vs = new Array(args.length);
          for (let i = 0; i < args.length; i++) {
            const v = args[i]!(row);
            if (v === null) return null;
            vs[i] = v;
          }
          return impl(...vs);
        };
    }
  };
}

const describeTypes = (ts: readonly DataType[]) => `(${ts.join(", ")})`;

/** A resolver choosing the first overload the argument types match exactly, else the
 * first they match with implicit widening. */
export function overloads(list: readonly Overload[], name?: string): (types: readonly DataType[]) => BoundFunction {
  return (types) => {
    const fits = (o: Overload, exact: boolean) => {
      if (types.length < o.args.length || (types.length > o.args.length && o.rest === undefined)) return false;
      return types.every((t, i) => {
        const want = o.args[i] ?? o.rest!;
        return exact ? t === want || t === "null" : widensTo(t, want);
      });
    };
    const o = list.find((x) => fits(x, true)) ?? list.find((x) => fits(x, false));
    if (!o) {
      const wanted = list.map((x) => describeTypes(x.rest ? [...x.args, `${x.rest}...` as DataType] : x.args)).join(" or ");
      throw new TypeMismatch(`${name ?? "function"} expects ${wanted}, got ${describeTypes(types)}`);
    }
    const argTypes = types.map((_, i) => o.args[i] ?? o.rest!);
    const compile = o.compile ?? (o.impl && strict(o.impl));
    if (!compile) throw internal(`${name ?? "a function"} has an overload with neither impl nor compile`);
    return { type: o.returns, argTypes, compile };
  };
}

// --- building calls -----------------------------------------------------------------

/** `e` as type `t`: unchanged if it already is, a literal retyped, else an implicit cast. */
export function coerce(e: Expr, t: DataType): Expr {
  if (e.type === t || t === "null") return e;
  if (e instanceof Literal) {
    if (e.value === null) return new Literal(null, t);
    if (e.type === "int" && t === "float") return new Literal(e.value, "float");
  }
  return new Cast(e, t, true);
}

/** A resolved call of the named function, with implicit casts on its arguments.
 * Throws TypeMismatch for bad arguments and Error for an unknown name. */
export function call(name: string, args: readonly Expr[]): Call {
  const fn = functions.get(name);
  if (!fn) throw new Error(`unknown function '${name}'`);
  return callOf(fn, args);
}

export function callOf(fn: ScalarFunction, args: readonly Expr[]): Call {
  const bound = fn.resolve(args.map((a) => a.type));
  return new Call(fn, args.map((a, i) => coerce(a, bound.argTypes[i] ?? a.type)), bound);
}

/** `a AND b AND ...`, or null for no expressions. */
export function andAll(exprs: readonly (Expr | null)[]): Expr | null {
  let out: Expr | null = null;
  for (const e of exprs) if (e) out = out ? call("and", [out, e]) : e;
  return out;
}

