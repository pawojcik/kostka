// The page's side of the worker: call(name, ...args) -> Promise of the value. Calls
// run one at a time in the worker; progress events of a call go to its `onEvent`.

type Pending = { resolve: (v: any) => void; reject: (e: Error) => void; onEvent?: (e: any) => void };

declare const __BUILD__: string;
// The build stamp keeps a browser from running a cached worker of an older build.
const worker = new Worker(new URL(`./worker.js?v=${typeof __BUILD__ === "string" ? __BUILD__ : "dev"}`, import.meta.url), { type: "module" });
const pending = new Map<number, Pending>();
let nextId = 1;
let onReady: (info: { ok: boolean; storage?: string; error?: string }) => void = () => {};

export const ready = new Promise<{ ok: boolean; storage?: string; error?: string }>((r) => (onReady = r));

worker.onmessage = (e: MessageEvent) => {
  const m = e.data;
  if (m.ready !== undefined) return onReady({ ok: m.ready, storage: m.storage, error: m.error });
  const p = pending.get(m.id);
  if (!p) return;
  if (m.event) return p.onEvent?.(m.event);
  pending.delete(m.id);
  if (m.ok) p.resolve(m.value);
  else p.reject(Object.assign(new Error(m.error), { name: m.errorName ?? "Error" }));
};

// Calls the page waits for, by name, so it can show that it is waiting. The editor's
// as-you-type checks are not waits.
const QUIET = new Set(["check"]);
const active: string[] = [];
let onActive: (names: readonly string[]) => void = () => {};
export function onActivity(fn: (names: readonly string[]) => void): void {
  onActive = fn;
}

export function callWith<T = any>(onEvent: ((e: any) => void) | undefined, name: string, ...args: unknown[]): Promise<T> {
  const id = nextId++;
  const p = new Promise<T>((resolve, reject) => {
    pending.set(id, { resolve, reject, onEvent });
    worker.postMessage({ id, name, args });
  });
  if (QUIET.has(name)) return p;
  active.push(name);
  onActive(active);
  const done = () => {
    active.splice(active.indexOf(name), 1);
    onActive(active);
  };
  p.then(done, done);
  return p;
}

export function call<T = any>(name: string, ...args: unknown[]): Promise<T> {
  return callWith<T>(undefined, name, ...args);
}
