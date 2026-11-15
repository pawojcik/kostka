// Compiles a bound expression into an evaluator: a closure computing its value from a
// row. Each node compiles itself (Expression.compile); this supplies the context,
// which resolves column references to row slots, once, for the column layout the
// operator receives. An id that is not in the layout is an internal error.

import { internal, ExecutionError } from "./errors.ts";
import type { CompileContext, Expression, Field } from "./expr.ts";
import type { Evaluator } from "./functions/registry.ts";
import type { Row } from "./types.ts";

export type { Evaluator };
export type Layout = ReadonlyMap<number, number>;

export function layoutOf(columns: readonly Field[]): Layout {
  return new Map(columns.map((f, i) => [f.id, i]));
}

export function compileExpr(e: Expression, columns: readonly Field[] | Layout): Evaluator {
  const layout = columns instanceof Map ? columns : layoutOf(columns as readonly Field[]);
  const ctx: CompileContext = {
    slot(ref) {
      const slot = layout.get(ref.id);
      if (slot === undefined) {
        const have = Array.isArray(columns) ? (columns as Field[]).map((f) => f.label()).join(", ") : [...layout.keys()].join(", ");
        throw internal(`column ${ref} is not in the input [${have}]`);
      }
      return slot;
    },
    compile: (x) => x.compile(ctx),
  };
  return e.compile(ctx);
}

/** A predicate: keeps a row only when the condition is exactly TRUE. */
export function compilePredicate(e: Expression, columns: readonly Field[] | Layout): (row: Row) => boolean {
  if (e.type !== "bool" && e.type !== "null") throw new ExecutionError(`condition ${e} must be a bool, not ${e.type}`);
  const f = compileExpr(e, columns);
  return (row) => f(row) === true;
}

