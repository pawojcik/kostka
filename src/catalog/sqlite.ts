// The catalog talks to SQLite through this small interface, so the same code runs on
// Node's built-in node:sqlite and on sqlite-wasm in a browser worker (both are
// synchronous).

export type SqlValue = null | number | string | Uint8Array;
export type SqlRow = Record<string, SqlValue>;

export interface SqlDb {
  /** Runs one or more statements without parameters. */
  exec(sql: string): void;
  all(sql: string, params?: readonly SqlValue[]): SqlRow[];
  run(sql: string, params?: readonly SqlValue[]): { lastId: number; changes: number };
  close(): void;
}

export function one(db: SqlDb, sql: string, params?: readonly SqlValue[]): SqlRow | undefined {
  return db.all(sql, params)[0];
}
