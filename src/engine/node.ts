// A Database on Node: the catalog in SQLite (node:sqlite), data files in a folder.
// With no folder, both are in memory (tests).

import { join } from "node:path";
import { mkdirSync } from "node:fs";
import { openNodeSqlite } from "../catalog/node-sqlite.ts";
import { MemoryFileStore } from "../storage/file-store.ts";
import { NodeFileStore } from "../storage/node-file-store.ts";
import { Database } from "./database.ts";

export function openDatabase(root?: string): Database {
  if (!root) return new Database(openNodeSqlite(), new MemoryFileStore());
  mkdirSync(root, { recursive: true });
  return new Database(openNodeSqlite(join(root, "catalog.db")), new NodeFileStore(root));
}
