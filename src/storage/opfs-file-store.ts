// Data files in the browser's origin-private file system. Runs in a worker: files
// are written through sync access handles, to a temporary name and then moved.

import { FileNotFound, type FileStore } from "./file-store.ts";

type Dir = FileSystemDirectoryHandle;

export class OpfsFileStore implements FileStore {
  private readonly rootName: string;
  constructor(rootName = "data") {
    this.rootName = rootName;
  }

  private async dir(parts: string[], create: boolean): Promise<Dir> {
    let d = await (await navigator.storage.getDirectory()).getDirectoryHandle(this.rootName, { create: true });
    for (const p of parts) d = await d.getDirectoryHandle(p, { create });
    return d;
  }

  private split(path: string): [string[], string] {
    const parts = path.split("/");
    return [parts.slice(0, -1), parts[parts.length - 1]!];
  }

  async read(path: string): Promise<Uint8Array> {
    const [dirs, name] = this.split(path);
    try {
      const file = await (await (await this.dir(dirs, false)).getFileHandle(name)).getFile();
      return new Uint8Array(await file.arrayBuffer());
    } catch (e) {
      if ((e as DOMException).name === "NotFoundError") throw new FileNotFound(path);
      throw e;
    }
  }

  async write(path: string, bytes: Uint8Array): Promise<void> {
    const [dirs, name] = this.split(path);
    const d = await this.dir(dirs, true);
    const tmp = await d.getFileHandle(`${name}.tmp`, { create: true });
    const h = await (tmp as unknown as { createSyncAccessHandle(): Promise<{ truncate(n: number): void; write(b: Uint8Array, o: { at: number }): void; flush(): void; close(): void }> }).createSyncAccessHandle();
    try {
      h.truncate(0);
      h.write(bytes, { at: 0 });
      h.flush();
    } finally {
      h.close();
    }
    await (tmp as unknown as { move(n: string): Promise<void> }).move(name);
  }

  async remove(path: string): Promise<void> {
    const [dirs, name] = this.split(path);
    try {
      await (await this.dir(dirs, false)).removeEntry(name);
    } catch {
      // already gone
    }
  }

  async removeDir(dir: string): Promise<void> {
    const [parents, name] = this.split(dir);
    try {
      await (await this.dir(parents, false)).removeEntry(name, { recursive: true });
    } catch {
      // already gone
    }
  }

  async usage(): Promise<number> {
    const walk = async (d: Dir): Promise<number> => {
      let n = 0;
      for await (const h of (d as unknown as { values(): AsyncIterable<FileSystemHandle> }).values()) {
        n += h.kind === "directory" ? await walk(h as Dir) : (await (h as FileSystemFileHandle).getFile()).size;
      }
      return n;
    };
    return walk(await this.dir([], true));
  }
}
