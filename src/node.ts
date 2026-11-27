// A Database on Node, with every engine there is: the catalog in SQLite (node:sqlite),
// data files in a folder. With no folder, both are in memory (tests). This is where
// the shared layers meet the engines (src/engines/index.ts), so it sits outside them.

import { join } from "node:path";
import { mkdirSync } from "node:fs";
import { openNodeSqlite } from "./catalog/node-sqlite.ts";
import { MemoryFileStore } from "./storage/file-store.ts";
import { NodeFileStore } from "./storage/node-file-store.ts";
import { Database } from "./api/database.ts";
import { ENGINES } from "./engines/index.ts";

export function openDatabase(root?: string): Database {
  if (!root) return new Database(openNodeSqlite(), new MemoryFileStore(), ENGINES);
  mkdirSync(root, { recursive: true });
  return new Database(openNodeSqlite(join(root, "catalog.db")), new NodeFileStore(root), ENGINES);
}
