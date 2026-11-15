// Where data files live. Paths are relative, '/'-separated: "<table folder>/<file>".
// Implementations: in memory (tests, a page without OPFS), the local file system
// (node-file-store.ts) and the browser's origin-private file system (opfs-file-store.ts).
// Writes are atomic: a reader sees the whole file or none.

export interface FileStore {
  read(path: string): Promise<Uint8Array>;
  write(path: string, bytes: Uint8Array): Promise<void>;
  remove(path: string): Promise<void>;
  /** Removes a folder and everything in it; nothing if it is missing. */
  removeDir(dir: string): Promise<void>;
  /** Total bytes stored (for the workbench). */
  usage?(): Promise<number>;
}

export class FileNotFound extends Error {
  constructor(path: string) {
    super(`no file ${path}`);
    this.name = "FileNotFound";
  }
}

export class MemoryFileStore implements FileStore {
  readonly files = new Map<string, Uint8Array>();
  async read(path: string): Promise<Uint8Array> {
    const f = this.files.get(path);
    if (!f) throw new FileNotFound(path);
    return f;
  }
  async write(path: string, bytes: Uint8Array): Promise<void> {
    this.files.set(path, bytes);
  }
  async remove(path: string): Promise<void> {
    this.files.delete(path);
  }
  async removeDir(dir: string): Promise<void> {
    for (const p of [...this.files.keys()]) if (p.startsWith(`${dir}/`)) this.files.delete(p);
  }
  async usage(): Promise<number> {
    let n = 0;
    for (const f of this.files.values()) n += f.length;
    return n;
  }
}

export function joinPath(...parts: string[]): string {
  return parts.filter(Boolean).join("/");
}
