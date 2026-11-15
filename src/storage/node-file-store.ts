// Data files in a folder on the local file system (Node).

import { mkdir, readFile, rename, rm, stat, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { FileNotFound, type FileStore } from "./file-store.ts";

export class NodeFileStore implements FileStore {
  readonly root: string;
  constructor(root: string) {
    this.root = root;
  }
  async read(path: string): Promise<Uint8Array> {
    try {
      return new Uint8Array(await readFile(join(this.root, path)));
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === "ENOENT") throw new FileNotFound(path);
      throw e;
    }
  }
  async write(path: string, bytes: Uint8Array): Promise<void> {
    const full = join(this.root, path);
    await mkdir(dirname(full), { recursive: true });
    await writeFile(`${full}.tmp`, bytes);
    await rename(`${full}.tmp`, full);
  }
  async remove(path: string): Promise<void> {
    await rm(join(this.root, path), { force: true });
  }
  async removeDir(dir: string): Promise<void> {
    await rm(join(this.root, dir), { recursive: true, force: true });
  }
  async usage(): Promise<number> {
    const walk = async (dir: string): Promise<number> => {
      let n = 0;
      for (const e of await readdir(dir, { withFileTypes: true }).catch(() => [])) {
        const p = join(dir, e.name);
        n += e.isDirectory() ? await walk(p) : (await stat(p)).size;
      }
      return n;
    };
    return walk(this.root);
  }
}
