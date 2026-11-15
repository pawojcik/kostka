// The error hierarchy. Every error kostka raises on purpose is a KostkaError; anything
// else is a bug.
//
//   KostkaError
//     CatalogError          the set of tables: TableExistsError, TableNotFoundError
//     SchemaError           a bad table definition, row or CSV value on load
//     SqlError              a query that cannot run: ParseError (syntax), AnalysisError
//                           (unknown names, type errors, misplaced aggregates)
//     ExecutionError        runtime errors: division by zero, bad casts, int overflow
//       CommitConflictError an append or replace kept losing races with other writers
//       OutOfMemoryError    a simulated worker could not fit a hash table or groups
//       OutOfDiskError      a simulated worker's disk could not take a spilled piece

export class KostkaError extends Error {
  override name = "KostkaError";
}

export class CatalogError extends KostkaError {
  override name = "CatalogError";
}
export class TableExistsError extends CatalogError {
  override name = "TableExistsError";
}
export class TableNotFoundError extends CatalogError {
  override name = "TableNotFoundError";
}

export class SchemaError extends KostkaError {
  override name = "SchemaError";
}

/** Where in the SQL text an error is: [start, end) character offsets. */
export type Span = readonly [start: number, end: number];

/** An error in a query's text. `span`, when known, is the part of the SQL at fault. */
export class SqlError extends KostkaError {
  override name = "SqlError";
  readonly span: Span | null;
  constructor(message: string, span: Span | null = null) {
    super(message);
    this.span = span;
  }
}
export class ParseError extends SqlError {
  override name = "ParseError";
}
export class AnalysisError extends SqlError {
  override name = "AnalysisError";
}

export class ExecutionError extends KostkaError {
  override name = "ExecutionError";
}
export class CommitConflictError extends ExecutionError {
  override name = "CommitConflictError";
}
export class OutOfMemoryError extends ExecutionError {
  override name = "OutOfMemoryError";
}

/** A simulated worker's local disk could not take a spilled piece. */
export class OutOfDiskError extends ExecutionError {
  override name = "OutOfDiskError";
}

/** An internal error: a broken invariant, never the user's fault. */
export function internal(message: string): ExecutionError {
  return new ExecutionError(`internal error: ${message}`);
}
