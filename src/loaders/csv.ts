// Loading CSV files: parse, infer a schema the caller can adjust, then write the rows
// into a table as one snapshot (create, replace or append).
//
// The whole file is parsed (RFC 4180: quoted fields may hold the delimiter, quotes
// and newlines), and a column's type is inferred from all of its values: the first of
// int, float, bool, date, timestamp that every non-null value parses as; else string.
//
// Numbers may use a decimal comma and a thousands separator ("1.234,5"), and dates and
// timestamps may follow a format pattern ("DD/MM/YYYY", "M/D/YYYY h:mm A") instead of
// ISO 8601. Pattern tokens: YYYY YY (year), MMM (Jan…Dec) MM M (month), DD D (day),
// HH H (hour 0-23), hh h (hour 1-12, with A for AM/PM), mm (minute), ss (second),
// S… (fraction of a second, any number of digits), Z (offset or Z); other characters
// match themselves. Times without an offset are taken as UTC, as in ISO text.

import { SchemaError } from "../core/errors.ts";
import { DAY_US, INT_MAX, isValidIdentifier, parseDate, parseTimestamp, type ColumnType, type Value } from "../core/types.ts";

export const NULL_VALUES = ["", "NULL", "null", "NA", "N/A"];
export const SAMPLE_ROWS = 20;

export interface CsvOptions {
  delimiter?: string;
  header?: boolean;
  nullValues?: string[];
  /** The decimal separator: "." (default) or ",". */
  decimal?: "." | ",";
  /** A thousands separator in numbers ("", the default, for none). */
  thousands?: "" | "," | "." | " " | "'";
  /** A pattern for date columns ("DD.MM.YYYY"); ISO 8601 when empty. */
  dateFormat?: string;
  /** A pattern for timestamp columns ("DD/MM/YYYY HH:mm:ss"); ISO 8601 when empty. */
  timestampFormat?: string;
}

const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const TOKENS: [string, string][] = [
  ["YYYY", "(\\d{4})"], ["YY", "(\\d{2})"], ["MMM", "([A-Za-z]{3})[A-Za-z]*\\.?"], ["MM", "(\\d{2})"], ["M", "(\\d{1,2})"],
  ["DD", "(\\d{2})"], ["D", "(\\d{1,2})"], ["HH", "(\\d{2})"], ["H", "(\\d{1,2})"], ["hh", "(\\d{2})"], ["h", "(\\d{1,2})"],
  ["mm", "(\\d{2})"], ["ss", "(\\d{2})"], ["S", "(\\d{1,9})"], ["A", "([AaPp][Mm])"], ["a", "([AaPp][Mm])"], ["Z", "(Z|[+-]\\d{2}(?::?\\d{2})?)"],
];

/** Compiles a date/time pattern into a parser of micros since the epoch (null if the
 * text doesn't match or isn't a real date). Throws on a pattern without a year, month
 * and day. */
export function formatParser(pattern: string): (text: string) => number | null {
  let re = "";
  const fields: string[] = [];
  for (let i = 0; i < pattern.length;) {
    const tok = TOKENS.find(([t]) => pattern.startsWith(t, i));
    if (tok) {
      let t = tok[0];
      if (t === "S") while (pattern[i + t.length] === "S") t += "S";
      re += tok[1];
      fields.push(tok[0]);
      i += t.length;
    } else {
      re += pattern[i]!.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/^ $/, "\\s+");
      i++;
    }
  }
  const has = (...t: string[]) => t.some((x) => fields.includes(x));
  if (!has("YYYY", "YY") || !has("MMM", "MM", "M") || !has("DD", "D")) throw new SchemaError(`the date format '${pattern}' needs a year, a month and a day (YYYY, MM, DD)`);
  const rx = new RegExp(`^\\s*${re}\\s*$`);
  return (text) => {
    const m = rx.exec(text);
    if (!m) return null;
    let y = 0, mo = 0, d = 0, h = 0, mi = 0, sec = 0, frac = "", pm: boolean | null = null, zone = "";
    fields.forEach((f, k) => {
      const v = m[k + 1]!;
      if (f === "YYYY") y = +v;
      else if (f === "YY") y = 2000 + +v - (+v >= 70 ? 100 : 0);
      else if (f === "MMM") mo = MONTHS.indexOf(v.toLowerCase()) + 1;
      else if (f === "MM" || f === "M") mo = +v;
      else if (f === "DD" || f === "D") d = +v;
      else if (f === "HH" || f === "H" || f === "hh" || f === "h") h = +v;
      else if (f === "mm") mi = +v;
      else if (f === "ss") sec = +v;
      else if (f === "S") frac = v;
      else if (f === "A" || f === "a") pm = v.toLowerCase() === "pm";
      else if (f === "Z") zone = v;
    });
    if (pm !== null) {
      if (h < 1 || h > 12) return null;
      h = (h % 12) + (pm ? 12 : 0);
    }
    if (mo < 1 || mo > 12 || h > 23 || mi > 59 || sec > 59) return null;
    const iso = `${String(y).padStart(4, "0")}-${String(mo).padStart(2, "0")}-${String(d).padStart(2, "0")}T${String(h).padStart(2, "0")}:${String(mi).padStart(2, "0")}:${String(sec).padStart(2, "0")}${frac ? `.${frac}` : ""}${zone}`;
    return parseTimestamp(iso);
  };
}

export interface CsvColumn {
  /** The header in the file (or column_N). */
  source: string;
  name: string;
  type: ColumnType;
  include: boolean;
}

export interface CsvInference {
  columns: CsvColumn[];
  rows: number;
  bytes: number;
  sample: Value[][];
}

export function parseCsv(text: string, delimiter = ","): string[][] {
  const rows: string[][] = [];
  let row: string[] = [], field = "", i = 0, quoted = false, any = false;
  const n = text.length;
  if (text.charCodeAt(0) === 0xfeff) i = 1;
  while (i < n) {
    const ch = text[i]!;
    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        quoted = false;
        i++;
        continue;
      }
      field += ch;
      i++;
      continue;
    }
    if (ch === '"' && field === "") {
      quoted = true;
      any = true;
      i++;
    } else if (ch === delimiter) {
      row.push(field);
      field = "";
      any = true;
      i++;
    } else if (ch === "\n" || ch === "\r") {
      if (any || field !== "" || row.length) {
        row.push(field);
        rows.push(row);
      }
      row = [];
      field = "";
      any = false;
      i += ch === "\r" && text[i + 1] === "\n" ? 2 : 1;
    } else {
      field += ch;
      any = true;
      i++;
    }
  }
  if (quoted) throw new SchemaError("cannot read the CSV file: a quoted field is not closed");
  if (any || field !== "" || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

const INT = /^[+-]?\d+$/;
const FLOAT = /^[+-]?(\d+\.?\d*([eE][+-]?\d+)?|\.\d+([eE][+-]?\d+)?|inf|infinity|nan)$/i;
const BOOL = /^(true|false)$/i;
const DATE = /^\d{4}-\d{2}-\d{2}$/;

type Parsers = Record<ColumnType, (s: string) => Value | undefined>;

/** Value parsers for the file's number and date conventions. */
function parsers(o: CsvOptions): Parsers {
  const dec = o.decimal ?? ".";
  const th = o.thousands ?? "";
  if (th && th === dec) throw new SchemaError("the decimal and thousands separators must differ");
  // A grouped number: digits in groups of three after the first ("1.234.567").
  const grouped = th ? new RegExp(`^[+-]?\\d{1,3}(${th.replace(/[.\\]/g, "\\$&")}\\d{3})+(?=$|[${dec === "." ? "." : ","}eE])`) : null;
  // The number in JavaScript's notation, or null if a separator is out of place.
  const plain = (s: string): string | null => {
    let t = s;
    if (grouped) { const g = grouped.exec(t); if (g) t = g[0].split(th).join("") + t.slice(g[0].length); }
    if (th && t.includes(th)) return null;
    if (dec === ",") return t.includes(".") ? null : t.replace(",", ".");
    return t;
  };
  const date = o.dateFormat ? formatParser(o.dateFormat) : null;
  const ts = o.timestampFormat ? formatParser(o.timestampFormat) : null;
  return {
    int: (s) => {
      const t = plain(s);
      if (t === null || !INT.test(t)) return undefined;
      const v = Number(t);
      return Math.abs(v) <= INT_MAX ? v : undefined;
    },
    float: (s) => {
      const t = plain(s);
      if (t === null || !FLOAT.test(t)) return undefined;
      const l = t.toLowerCase().replace(/^\+/, "");
      if (l === "nan") return NaN;
      if (l === "inf" || l === "infinity") return Infinity;
      if (l === "-inf" || l === "-infinity") return -Infinity;
      return Number(t);
    },
    bool: (s) => (BOOL.test(s) ? s.toLowerCase() === "true" : undefined),
    date: date
      ? (s) => { const t = date(s); return t === null ? undefined : Math.floor(t / DAY_US); }
      : (s) => (DATE.test(s) ? (parseDate(s) ?? undefined) : undefined),
    timestamp: ts
      ? (s) => ts(s) ?? undefined
      : (s) => (/^\d{4}-\d{2}-\d{2}[ T]\d/.test(s) ? (parseTimestamp(s) ?? undefined) : undefined),
    string: (s) => s,
  };
}

const ORDER: ColumnType[] = ["int", "float", "bool", "date", "timestamp"];

function inferType(values: readonly (string | null)[], P: Parsers): ColumnType {
  const present = values.filter((v): v is string => v !== null);
  if (!present.length) return "string";
  for (const t of ORDER) if (present.every((v) => P[t](v.trim()) !== undefined)) return t;
  return "string";
}

/** A valid, unused kostka column name made from a CSV header. */
export function columnName(header: string, index: number, taken: Set<string>): string {
  let name = header.trim().replace(/\W/g, "_").replace(/^_+|_+$/g, "").toLowerCase() || `column_${index + 1}`;
  if (!isValidIdentifier(name)) name = `c_${name}`;
  const base = name;
  for (let k = 2; taken.has(name); k++) name = `${base}_${k}`;
  taken.add(name);
  return name;
}

interface Parsed {
  headers: string[];
  body: (string | null)[][];
}

function read(text: string, o: CsvOptions): Parsed {
  const rows = parseCsv(text, o.delimiter ?? ",");
  const nulls = new Set(o.nullValues ?? NULL_VALUES);
  // A loop, not Math.max(...rows): spreading a big file's rows overflows the stack.
  let width = 0;
  for (const r of rows) if (r.length > width) width = r.length;
  const header = o.header ?? true;
  const headers = header && rows.length ? rows[0]!.concat(Array(Math.max(0, width - rows[0]!.length)).fill("")).map((h, i) => h || `column_${i + 1}`) : Array.from({ length: width }, (_, i) => `column_${i + 1}`);
  const body = (header ? rows.slice(1) : rows).map((r) => headers.map((_, i) => (i < r.length && !nulls.has(r[i]!) ? r[i]! : null)));
  return { headers, body };
}

export function inferCsv(text: string, o: CsvOptions = {}): CsvInference {
  const { headers, body } = read(text, o);
  const P = parsers(o);
  const taken = new Set<string>();
  const columns = headers.map((h, i) => ({ source: h, name: columnName(h, i, taken), type: inferType(body.map((r) => r[i]!), P), include: true }));
  return {
    columns, rows: body.length, bytes: new TextEncoder().encode(text).length,
    sample: body.slice(0, SAMPLE_ROWS).map((r) => r.map((v, i) => (v === null ? null : (P[columns[i]!.type](v.trim()) ?? v)))),
  };
}

/** The included columns' values, converted to their chosen types, row by row.
 * A value that doesn't parse as its column's type is an error naming the line. */
export function* csvRows(text: string, columns: readonly CsvColumn[], o: CsvOptions = {}): Generator<Value[]> {
  const { headers, body } = read(text, o);
  const P = parsers(o);
  const specs = columns.filter((c) => c.include);
  if (!specs.length) throw new SchemaError("choose at least one column to import");
  const idx = specs.map((c) => {
    const i = headers.indexOf(c.source);
    if (i < 0) throw new SchemaError(`the CSV file has no column '${c.source}'`);
    return i;
  });
  const first = (o.header ?? true) ? 2 : 1;
  for (let r = 0; r < body.length; r++) {
    const row = body[r]!;
    yield specs.map((c, j) => {
      const raw = row[idx[j]!]!;
      if (raw === null) return null;
      const v = P[c.type](c.type === "string" ? raw : raw.trim());
      if (v === undefined) throw new SchemaError(`line ${r + first}: '${raw}' in column ${c.source} is not a ${c.type}`);
      return v;
    });
  }
}
