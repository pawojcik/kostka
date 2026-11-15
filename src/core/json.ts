// Plain data from live objects: what can cross to the page (structured clone) and be
// kept unchanged while the objects it came from go on changing.

export function toJson(v: unknown, depth = 0): unknown {
  if (v === null || typeof v !== "object") return v;
  if (depth > 4) return String(v);
  if (Array.isArray(v)) return v.slice(0, 50).map((x) => toJson(x, depth + 1));
  if (v instanceof Map) return Object.fromEntries([...v].slice(0, 50).map(([k, x]) => [String(k), toJson(x, depth + 1)]));
  if (v instanceof Set) return [...v].slice(0, 50).map((x) => toJson(x, depth + 1));
  if (typeof (v as { display?: unknown }).display === "function") return (v as { display(): string }).display();
  return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toJson(x, depth + 1)]));
}
