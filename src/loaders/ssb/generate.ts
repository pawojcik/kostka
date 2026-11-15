// Star Schema Benchmark data (O'Neil, O'Neil, Chen 2009): one fact table, lineorder, and
// four dimensions, date, customer, supplier and part. `generateSsb` fills them with
// seeded random data whose keys and value domains follow the SSB specification, so the
// 13 standard queries (queries.ts) mean what they mean on dbgen data. Distributions are
// simpler than dbgen's.
//
// Row counts at scale factor sf: lineorder 6,000,000 x sf, customer 30,000 x sf (at least
// 1,000), supplier 2,000 x sf (at least 100), part 200,000 x sf (at least 2,000), and date
// 2,557 days, 1992-01-01 to 1998-12-31. The minimums keep the selective queries
// non-empty at small scales.
//
// Keys of date are yyyymmdd integers, as in SSB. Money columns are integers (cents),
// so sums are exact. Orders are generated in date order, so lineorder files cover
// narrow date ranges and file pruning works on lo_orderdate.

import { DAY_MS, daysOf, type ColumnType, type Value } from "../../core/types.ts";
import type { ColumnDef } from "../../catalog/meta.ts";
import { Random } from "../random.ts";
import type { OpenTable } from "../tpcds/generate.ts";

export const SCALES = [0.01, 0.03, 0.1, 0.3] as const;

const col = (spec: string): ColumnDef[] => spec.trim().split(/,\s*/).map((c) => {
  const [name, t] = c.split(" ");
  return { name: name!, type: ({ i: "int", s: "string" } as Record<string, ColumnType>)[t!]! };
});

export const SCHEMA: Record<string, ColumnDef[]> = {
  lineorder: col(`lo_orderkey i, lo_linenumber i, lo_custkey i, lo_partkey i, lo_suppkey i, lo_orderdate i,
    lo_orderpriority s, lo_shippriority i, lo_quantity i, lo_extendedprice i, lo_ordtotalprice i, lo_discount i,
    lo_revenue i, lo_supplycost i, lo_tax i, lo_commitdate i, lo_shipmode s`),
  date: col(`d_datekey i, d_date s, d_dayofweek s, d_month s, d_year i, d_yearmonthnum i, d_yearmonth s,
    d_daynuminweek i, d_daynuminmonth i, d_daynuminyear i, d_monthnuminyear i, d_weeknuminyear i,
    d_sellingseason s, d_lastdayinweekfl i, d_lastdayinmonthfl i, d_holidayfl i, d_weekdayfl i`),
  customer: col(`c_custkey i, c_name s, c_address s, c_city s, c_nation s, c_region s, c_phone s, c_mktsegment s`),
  supplier: col(`s_suppkey i, s_name s, s_address s, s_city s, s_nation s, s_region s, s_phone s`),
  part: col(`p_partkey i, p_name s, p_mfgr s, p_category s, p_brand1 s, p_color s, p_type s, p_size i, p_container s`),
};
export const TABLES = ["date", "customer", "supplier", "part", "lineorder"];

/** The 25 TPC-H nations, each with its region. */
const NATIONS: [string, string][] = [
  ["ALGERIA", "AFRICA"], ["ARGENTINA", "AMERICA"], ["BRAZIL", "AMERICA"], ["CANADA", "AMERICA"], ["EGYPT", "MIDDLE EAST"],
  ["ETHIOPIA", "AFRICA"], ["FRANCE", "EUROPE"], ["GERMANY", "EUROPE"], ["INDIA", "ASIA"], ["INDONESIA", "ASIA"],
  ["IRAN", "MIDDLE EAST"], ["IRAQ", "MIDDLE EAST"], ["JAPAN", "ASIA"], ["JORDAN", "MIDDLE EAST"], ["KENYA", "AFRICA"],
  ["MOROCCO", "AFRICA"], ["MOZAMBIQUE", "AFRICA"], ["PERU", "AMERICA"], ["CHINA", "ASIA"], ["ROMANIA", "EUROPE"],
  ["SAUDI ARABIA", "MIDDLE EAST"], ["VIETNAM", "ASIA"], ["RUSSIA", "EUROPE"], ["UNITED KINGDOM", "EUROPE"], ["UNITED STATES", "AMERICA"],
];
const SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];
const PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECI", "5-LOW"];
const SHIP_MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
const COLORS = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown",
  "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark",
  "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green",
  "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
  "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange",
  "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal",
  "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle",
  "tomato", "turquoise", "violet", "wheat", "white", "yellow"];
const TYPES: string[][] = [["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"],
  ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"], ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]];
const CONTAINERS: string[][] = [["SM", "LG", "MED", "JUMBO", "WRAP"], ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]];
const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const STREET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
const HOLIDAYS = new Set(["0101", "0704", "1225", "1231", "0214", "1111", "1031"]);

const DATE_FIRST = daysOf(1992, 1, 1), DATE_LAST = daysOf(1998, 12, 31);
const ORDER_LAST = daysOf(1998, 8, 2); // the last order date, as in TPC-H

/** The SSB city of a nation: its first 9 letters padded to 9, then a digit 0-9. */
export const city = (nation: string, n: number): string => nation.slice(0, 9).padEnd(9, " ") + n;

const civil = (days: number) => {
  const d = new Date(days * DAY_MS);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), dow: d.getUTCDay() };
};
/** A date as SSB's yyyymmdd integer key. */
export const dateKey = (days: number): number => {
  const c = civil(days);
  return c.y * 10000 + c.m * 100 + c.d;
};

/** A part's retail price in cents, by the TPC-H formula. */
const retailPrice = (p: number) => 90000 + (Math.floor(p / 10) % 20001) + 100 * (p % 1000);

export function rowCounts(scale: number): Record<string, number> {
  return {
    lineorder: Math.max(1000, Math.round(6_000_000 * scale)), date: DATE_LAST - DATE_FIRST + 1,
    customer: Math.max(1000, Math.round(30_000 * scale)), supplier: Math.max(100, Math.round(2_000 * scale)),
    part: Math.max(2000, Math.round(200_000 * scale)),
  };
}

export interface SsbOptions {
  seed?: number;
  /** Called as rows are written: (table, rows so far, rows expected). */
  report?: (table: string, done: number, total: number) => void;
  log?: (text: string) => void;
}

/** Creates and fills the five SSB tables, replacing any with the same names. Returns
 * the row counts. */
export async function generateSsb(open: OpenTable, scale = 0.1, o: SsbOptions = {}): Promise<Record<string, number>> {
  if (!(scale > 0 && scale <= 1)) throw new RangeError("scale must be in (0, 1]");
  const counts = rowCounts(scale), seed = o.seed ?? 0, report = o.report ?? (() => {});
  const rnd = (t: string) => new Random(`ssb/${seed}/${t}`);
  const phone = (r: Random, nation: number) => `${10 + nation}-${r.int(100, 999)}-${r.int(100, 999)}-${r.int(1000, 9999)}`;
  const address = (r: Random) => Array.from({ length: r.int(10, 25) }, () => STREET[r.int(0, STREET.length - 1)]).join("").trim() || "x";

  const fill = async (table: string, rows: (add: (row: Value[]) => Promise<void>) => Promise<void>) => {
    const t0 = performance.now();
    const sink = await open(table, SCHEMA[table]!, `SSB scale ${scale}`);
    let buf: Value[][] = [], done = 0;
    const push = async () => {
      await sink.add(buf);
      done += buf.length;
      buf = [];
      report(table, done, counts[table]!);
    };
    report(table, 0, counts[table]!);
    try {
      await rows(async (row) => {
        buf.push(row);
        if (buf.length >= 2048) await push();
      });
      if (buf.length) await push();
      await sink.commit();
    } catch (e) {
      await sink.abort();
      throw e;
    }
    counts[table] = done;
    o.log?.(`${table}: ${done.toLocaleString("en-US")} rows, ${SCHEMA[table]!.length} columns (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
  };

  await fill("date", async (add) => {
    for (let day = DATE_FIRST; day <= DATE_LAST; day++) {
      const c = civil(day), doy = day - daysOf(c.y, 1, 1) + 1, last = civil(day + 1).m !== c.m;
      const season = c.m === 12 ? "Christmas" : c.m >= 6 && c.m <= 8 ? "Summer" : c.m <= 2 ? "Winter" : c.m <= 5 ? "Spring" : "Fall";
      const md = String(c.m * 100 + c.d).padStart(4, "0");
      await add([dateKey(day), `${MONTHS[c.m - 1]} ${c.d}, ${c.y}`, DAYS[c.dow]!, MONTHS[c.m - 1]!, c.y, c.y * 100 + c.m,
        `${MONTHS[c.m - 1]!.slice(0, 3)}${c.y}`, c.dow + 1, c.d, doy, c.m, Math.floor((doy - 1) / 7) + 1, season,
        c.dow === 6 ? 1 : 0, last ? 1 : 0, HOLIDAYS.has(md) ? 1 : 0, c.dow > 0 && c.dow < 6 ? 1 : 0]);
    }
  });

  for (const [table, prefix] of [["customer", "Customer"], ["supplier", "Supplier"]] as const) {
    const r = rnd(table);
    await fill(table, async (add) => {
      for (let k = 1; k <= counts[table]!; k++) {
        const n = r.int(0, 24), [nation, region] = NATIONS[n]!;
        const row: Value[] = [k, `${prefix}#${String(k).padStart(9, "0")}`, address(r), city(nation, r.int(0, 9)), nation, region, phone(r, n)];
        if (table === "customer") row.push(r.choice(SEGMENTS));
        await add(row);
      }
    });
  }

  const pr = rnd("part");
  await fill("part", async (add) => {
    // Every one of the 1,000 brands gets parts, in random order, so the one-brand queries find some.
    const brands = pr.shuffle(Array.from({ length: counts.part! }, (_, i) => i % 1000));
    for (let k = 1; k <= counts.part!; k++) {
      const x = brands[k - 1]!, m = Math.floor(x / 200) + 1, c = (Math.floor(x / 40) % 5) + 1, b = (x % 40) + 1;
      await add([k, `${pr.choice(COLORS)} ${pr.choice(COLORS)}`, `MFGR#${m}`, `MFGR#${m}${c}`, `MFGR#${m}${c}${b}`,
        pr.choice(COLORS), TYPES.map((t) => pr.choice(t)).join(" "), pr.int(1, 50), CONTAINERS.map((t) => pr.choice(t)).join(" ")]);
    }
  });

  const lr = rnd("lineorder"), target = counts.lineorder!, orders = Math.ceil(target / 4), span = ORDER_LAST - DATE_FIRST + 1;
  await fill("lineorder", async (add) => {
    let written = 0;
    for (let ok = 1; written < target; ok++) {
      const day = DATE_FIRST + Math.min(span - 1, Math.floor(((ok - 1) * span) / orders));
      const cust = lr.int(1, counts.customer!), prio = lr.choice(PRIORITIES), lines = Math.min(lr.int(1, 7), target - written);
      const items = Array.from({ length: lines }, () => {
        const part = lr.int(1, counts.part!), q = lr.int(1, 50), disc = lr.int(0, 10), tax = lr.int(0, 8);
        const ext = q * retailPrice(part);
        return { part, q, disc, tax, ext, supp: lr.int(1, counts.supplier!), commit: day + lr.int(30, 90), mode: lr.choice(SHIP_MODES) };
      });
      const total = items.reduce((s, it) => s + Math.floor((it.ext * (100 - it.disc) * (100 + it.tax)) / 10000), 0);
      for (let i = 0; i < lines; i++) {
        const it = items[i]!;
        await add([ok, i + 1, cust, it.part, it.supp, dateKey(day), prio, 0, it.q, it.ext, total, it.disc,
          Math.floor((it.ext * (100 - it.disc)) / 100), Math.floor((6 * retailPrice(it.part)) / 10), it.tax, dateKey(it.commit), it.mode]);
      }
      written += lines;
    }
  });
  return counts;
}
