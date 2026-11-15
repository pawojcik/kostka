// TPC-DS-shaped data for the workbench. `generateTpcds` creates the 24 tables of the
// TPC-DS schema and fills them with seeded random data whose keys and value domains
// follow the specification closely enough for the standard queries (queries.ts) to
// return rows. It is not dsdgen: distributions are simpler, and it is not suitable for
// publishing benchmark results.
//
// Row counts are the scale factor 1 counts times `scale`, except:
// - date_dim (73,049 days, 1900-01-02 to 2100-01-01), time_dim (86,400 seconds),
//   household_demographics (7,200), income_band, reason, ship_mode, call_center,
//   warehouse, web_site, web_page, store and promotion keep their fixed sizes;
// - customer_demographics keeps the first 1,920,800 x scale rows of the standard cross
//   product, varying gender fastest, so every gender/marital/education combination exists;
// - inventory has weekly snapshots of every 10th item, a tenth of the standard rows.
//
// Sales are generated in date order, so fact files cover narrow date ranges. Each
// channel's returns are about 10% of its sale lines and refer to them.

import { DAY_MS, daysOf, type Value } from "../../core/types.ts";
import type { ColumnDef } from "../../catalog/meta.ts";
import { Random, round } from "../random.ts";
import { SCHEMA, TABLES } from "./schema.ts";

export { SCHEMA, TABLES };

export interface TableSink {
  add(rows: Value[][]): Promise<void>;
  commit(): Promise<unknown>;
  abort(): Promise<void>;
}
/** Replaces (or creates) a table, as one new snapshot once committed. */
export type OpenTable = (table: string, columns: readonly ColumnDef[], detail: string) => Promise<TableSink>;

export const SCALES = [0.01, 0.03, 0.1, 0.3] as const;

const SF1_ROWS: Record<string, number> = {
  store_sales: 2_880_404, catalog_sales: 1_441_548, web_sales: 719_384, customer: 100_000,
  customer_address: 50_000, item: 18_000, catalog_page: 11_718, customer_demographics: 1_920_800,
};

const JD_EPOCH = 2_440_588; // the Julian day number of 1970-01-01: d_date_sk = days + this
const DATE_FIRST = daysOf(1900, 1, 2), DATE_LAST = daysOf(2100, 1, 1);
const SALES_FIRST = daysOf(1998, 1, 2), SALES_LAST = daysOf(2002, 12, 31);
const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const SYLLABLES = ["bar", "ought", "able", "pri", "ese", "anti", "cally", "ation", "eing", "n st"];
const STATES = ["TN", "GA", "TX", "KY", "VA", "OH", "IL", "IN", "MO", "NC", "MI", "IA", "WI", "MN", "NE", "KS", "MS", "AL", "AR", "LA", "OK", "SD", "ND", "MT", "CO", "WV", "PA", "NY", "CA", "WA", "FL", "SC", "ID", "NM", "OR", "UT", "AZ", "NV", "ME", "VT"];
const COUNTIES = ["Williamson County", "Franklin Parish", "Bronx County", "Orange County", "Ziebach County", "Walker County", "Daviess County", "Barrow County", "Luce County", "Richland County", "Fairfield County", "Huron County", "Kittitas County", "Jackson County", "Mobile County", "Raleigh County", "Pennington County", "Salem County", "Gogebic County", "Lea County", "Furnas County", "Dauphin County", "Maverick County", "Mesa County", "Terrell County", "Arthur County", "Oglethorpe County", "Wadena County", "Levy County", "Dade County", "Saginaw County", "Perry County"];
const CITIES = ["Midway", "Fairview", "Oak Grove", "Five Points", "Pleasant Hill", "Centerville", "Riverside", "Mount Zion", "Union", "Greenwood", "Liberty", "Salem", "Springdale", "Shiloh", "Oakland", "Clinton", "Glendale", "Concord", "Bethel", "Lakeside", "Antioch", "Georgetown", "Marion", "Newport", "Franklin", "Woodville"];
const STREETS = ["Main", "Oak", "Park", "Elm", "Cedar", "Maple", "Pine", "Lake", "Hill", "Washington", "Lincoln", "Jackson", "Sunset", "Railroad", "Church", "Mill", "Spring", "Ridge", "Forest", "Highland", "College", "Center", "View", "Woodland", "Willow", "Smith", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh"];
const STREET_TYPES = ["Street", "Avenue", "Road", "Boulevard", "Drive", "Lane", "Court", "Circle", "Way", "Parkway", "Ln", "Dr", "RD", "Blvd", "ST", "Ave", "Cir", "Ct", "Pkwy", "Wy"];
/** The zip codes TPC-DS query 15 looks for. */
const SPECIAL_ZIPS = ["85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"];
const FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Nancy", "Matthew", "Lisa", "Anthony", "Betty", "Mark", "Margaret", "Paul", "Sandra"];
const LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"];
const COUNTRIES = ["UNITED STATES", "CANADA", "MEXICO", "GERMANY", "FRANCE", "JAPAN", "BRAZIL", "INDIA", "CHINA", "ITALY", "SPAIN", "POLAND", "KENYA", "PERU", "NORWAY"];
const CATEGORIES: [string, string[]][] = [
  ["Women", ["dresses", "maternity", "swimwear", "fragrances"]],
  ["Men", ["pants", "shirts", "accessories", "sports-apparel"]],
  ["Children", ["infants", "toddlers", "school-uniforms", "newborn"]],
  ["Shoes", ["athletic", "kids", "mens", "womens"]],
  ["Music", ["classical", "country", "pop", "rock"]],
  ["Jewelry", ["birdal", "bracelets", "diamonds", "earings"]],
  ["Home", ["bedding", "furniture", "kids", "lighting"]],
  ["Sports", ["baseball", "camping", "fishing", "golf"]],
  ["Books", ["arts", "business", "computers", "fiction"]],
  ["Electronics", ["audio", "cameras", "dvd/vcr players", "monitors"]],
];
const BRAND_PREFIXES = ["amalg", "edu pack", "export", "import", "brand", "corp", "univ", "scholar", "maxi", "nameless"];
const BRAND_SUFFIXES = ["importo", "exporti", "amalg", "maxi", "brand", "corp", "univ", "scholar", "edu pack", "nameless"];
const COLORS = ["slate", "blanched", "burnished", "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blue", "brown", "chartreuse", "chiffon", "coral", "cornflower", "cream", "cyan", "dark", "deep", "dim", "dodger", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green"];
const SIZES = ["small", "medium", "large", "extra large", "petite", "N/A", "economy"];
const UNITS = ["Each", "Dozen", "Case", "Pallet", "Gross", "Lb", "Oz", "Ton", "Bundle", "Box", "Pound", "Cup", "Dram", "Gram", "Tbl", "Tsp", "Unknown", "Carton", "Ounce", "Bunch"];
const WORDS = ["able", "about", "again", "areas", "around", "become", "children", "company", "country", "different", "early", "economic", "family", "great", "group", "home", "important", "large", "later", "little", "local", "national", "number", "often", "other", "people", "political", "possible", "public", "really", "service", "small", "social", "special", "still", "system", "things", "today", "various", "young"];
const EDUCATION = ["Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree", "Advanced Degree", "Unknown"];
const BUY_POTENTIAL = [">10000", "5001-10000", "1001-5000", "501-1000", "0-500", "Unknown"];
const CREDIT = ["Good", "High Risk", "Low Risk", "Unknown"];
const CARRIERS = ["UPS", "FEDEX", "AIRBORNE", "USPS", "DHL", "TBS", "ZHOU", "ZOUROS", "MSC", "LATVIAN", "ALLIANCE", "ORIENTAL", "BARIAN", "BOXBUNDLES", "GREAT EASTERN", "DIAMOND", "RUPEKSA", "GERMA", "HARMSTORF", "PRIVATECARRIER"];
const CALL_CENTERS = ["NY Metro", "Mid Atlantic", "North Midwest", "California", "Pacific Northwest", "Hawaii/Alaska"];
const TAX_RATES = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09];
const NULL_FK = 0.02; // share of nulls in the non-key foreign keys of fact tables

export const dateSk = (days: number): number => days + JD_EPOCH;

/** A 16-letter business key, like dsdgen's 'AAAAAAAABAAAAAAA' for 1. */
export function bkey(n: number): string {
  let out = "AAAAAAAA";
  for (let i = 0; i < 8; i++) {
    out += String.fromCharCode(65 + (n & 15));
    n >>>= 4;
  }
  return out;
}

const syllables = (n: number) => [...String(n)].map((d) => SYLLABLES[Number(d)]).join("");
const civil = (days: number) => {
  const d = new Date(days * DAY_MS);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), dow: d.getUTCDay() };
};

const INVENTORY_DATES = Array.from({ length: 261 }, (_, w) => daysOf(1998, 1, 1) + w * 7);

/** The row count of every table at `scale` (approximate for the sales and returns). */
export function rowCounts(scale: number): Record<string, number> {
  const scaled = (t: string, lo = 1) => Math.max(lo, Math.round(SF1_ROWS[t]! * scale));
  const items = scaled("item", 100);
  const c: Record<string, number> = {
    call_center: 6, catalog_page: scaled("catalog_page", 100), customer: scaled("customer", 100),
    customer_address: scaled("customer_address", 50), customer_demographics: scaled("customer_demographics", 2 * 5 * 7 * 20),
    date_dim: DATE_LAST - DATE_FIRST + 1, household_demographics: 7200, income_band: 20, item: items, promotion: 300,
    reason: 35, ship_mode: 20, store: 12, time_dim: 86_400, warehouse: 5, web_page: 60, web_site: 30,
    store_sales: scaled("store_sales"), catalog_sales: scaled("catalog_sales"), web_sales: scaled("web_sales"),
    inventory: INVENTORY_DATES.length * 5 * Math.ceil(items / 10),
  };
  for (const ch of ["store", "catalog", "web"]) c[`${ch}_returns`] = Math.round(c[`${ch}_sales`]! * 0.1);
  return c;
}

/** Dimensions first, then each sales table followed by its returns. */
export function generationOrder(): string[] {
  return [...TABLES.filter((t) => !t.endsWith("_sales") && !t.endsWith("_returns")),
    "store_sales", "store_returns", "catalog_sales", "catalog_returns", "web_sales", "web_returns"];
}

export interface TpcdsOptions {
  seed?: number;
  /** Called as rows are written: (table, rows so far, rows expected). */
  report?: (table: string, done: number, total: number) => void;
  log?: (text: string) => void;
}

class Gen {
  readonly open: OpenTable;
  readonly scale: number;
  readonly seed: number;
  readonly counts: Record<string, number>;
  readonly zips: string[];
  readonly report: NonNullable<TpcdsOptions["report"]>;

  constructor(open: OpenTable, scale: number, o: TpcdsOptions) {
    this.open = open;
    this.scale = scale;
    this.seed = o.seed ?? 0;
    this.counts = rowCounts(scale);
    this.report = o.report ?? (() => {});
    this.zips = [...SPECIAL_ZIPS, ...new Random(this.seed).sample(10000, 99998, 400).map((z) => String(z).padStart(5, "0"))];
  }

  rnd(table: string): Random {
    return new Random(`${this.seed}/${table}`);
  }

  maybe<T>(rnd: Random, v: T, p = NULL_FK): T | null {
    return rnd.random() < p ? null : v;
  }

  words(rnd: Random, n: number): string {
    const s = Array.from({ length: n }, () => rnd.choice(WORDS)).join(" ");
    return s[0]!.toUpperCase() + s.slice(1);
  }

  address(rnd: Random): Value[] {
    return [String(rnd.int(1, 999)), `${rnd.choice(STREETS)} ${rnd.choice(STREETS)}`, rnd.choice(STREET_TYPES),
      `Suite ${rnd.int(0, 49) * 10}`, rnd.choice(CITIES), rnd.choice(COUNTIES), rnd.choice(STATES), rnd.choice(this.zips),
      "United States", rnd.choice([-5, -6, -7, -8, -9, -10])];
  }

  /** `n` values from 1..ids, each used about n/ids times, in random order. */
  balanced(rnd: Random, ids: number, n: number): number[] {
    return rnd.shuffle(Array.from({ length: n }, (_, i) => (i % ids) + 1));
  }

  /** The price columns the three sales tables share, as dsdgen derives them:
   * wholesale_cost .. ext_tax, then coupon. */
  prices(rnd: Random, q: number): number[] {
    const wc = round(rnd.uniform(1, 100));
    const lp = round(wc * (1 + rnd.uniform(0, 2)));
    const sp = round(lp * (1 - rnd.uniform(0, 1)));
    const ext = round(sp * q);
    const tax = round(ext * rnd.choice(TAX_RATES));
    const coupon = rnd.random() < 0.2 ? round(ext * rnd.random()) : 0;
    return [wc, lp, sp, round((lp - sp) * q), ext, round(wc * q), round(lp * q), tax, coupon];
  }

  async writer(table: string): Promise<{ add(row: Value[]): Promise<void>; flush(): Promise<number> }> {
    const sink = await this.open(table, SCHEMA[table]!, `TPC-DS scale ${this.scale}`);
    let buf: Value[][] = [], rows = 0;
    const push = async () => {
      await sink.add(buf);
      rows += buf.length;
      buf = [];
      this.report(table, rows, this.counts[table]!);
    };
    return {
      add: async (row) => {
        buf.push(row);
        if (buf.length >= 2048) await push();
      },
      flush: async () => {
        try {
          if (buf.length) await push();
          await sink.commit();
        } catch (e) {
          await sink.abort().catch(() => {});
          throw e;
        }
        return rows;
      },
    };
  }

  async simple(table: string, rows: Iterable<Value[]>): Promise<void> {
    const w = await this.writer(table);
    for (const r of rows) await w.add(r);
    this.counts[table] = await w.flush();
    this.report(table, this.counts[table]!, this.counts[table]!);
  }

  // --- dimensions ------------------------------------------------------------------

  *dateDim(): Generator<Value[]> {
    const holidays = new Set(["1-1", "7-4", "12-25", "11-11", "5-30", "9-1", "11-26", "2-14"]);
    const week0 = daysOf(1899, 12, 31); // a Sunday
    for (let day = DATE_FIRST; day <= DATE_LAST; day++) {
      const { y, m, d, dow } = civil(day);
      const sk = dateSk(day);
      const qoy = Math.floor((m - 1) / 3) + 1;
      const monthSeq = (y - 1900) * 12 + m - 1, quarterSeq = (y - 1900) * 4 + qoy;
      const weekSeq = Math.floor((day - week0) / 7) + 1;
      const first = daysOf(y, m, 1), last = daysOf(y, m + 1, 1) - 1;
      const prev = civil(day - 1);
      yield [sk, bkey(sk), day, monthSeq, weekSeq, quarterSeq, y, dow, m, d, qoy, y, quarterSeq, weekSeq, DAY_NAMES[dow]!,
        `${y}Q${qoy}`, holidays.has(`${m}-${d}`) ? "Y" : "N", dow === 0 || dow === 6 ? "Y" : "N",
        holidays.has(`${prev.m}-${prev.d}`) ? "Y" : "N", dateSk(first), dateSk(last), sk - 365, sk - 91, "N", "N", "N", "N", "N"];
    }
  }

  *timeDim(): Generator<Value[]> {
    for (let s = 0; s < 86_400; s++) {
      const h = Math.floor(s / 3600), mi = Math.floor(s / 60) % 60, sec = s % 60;
      const shift = h < 8 ? "third" : h < 16 ? "first" : "second";
      const sub = h < 6 ? "night" : h < 12 ? "morning" : h < 18 ? "afternoon" : "evening";
      const meal = h >= 6 && h < 9 ? "breakfast" : h >= 11 && h < 14 ? "lunch" : h >= 17 && h < 20 ? "dinner" : null;
      yield [s, bkey(s), s, h, mi, sec, h < 12 ? "AM" : "PM", shift, sub, meal];
    }
  }

  *householdDemographics(): Generator<Value[]> {
    let sk = 0;
    for (let ib = 1; ib <= 20; ib++) for (const bp of BUY_POTENTIAL) for (let dep = 0; dep < 10; dep++) for (let veh = -1; veh < 5; veh++) yield [++sk, ib, bp, dep, veh];
  }

  *customerDemographics(): Generator<Value[]> {
    const est = Array.from({ length: 20 }, (_, i) => 500 + i * 500);
    const seven = [0, 1, 2, 3, 4, 5, 6];
    const dims: Value[][] = [["M", "F"], ["M", "S", "D", "W", "U"], EDUCATION, est, CREDIT, seven, seven, seven];
    for (let i = 0; i < this.counts.customer_demographics!; i++) {
      const vals: Value[] = [i + 1];
      let n = i;
      for (const d of dims) {
        vals.push(d[n % d.length]!);
        n = Math.floor(n / d.length);
      }
      yield vals;
    }
  }

  *store(rnd: Random): Generator<Value[]> {
    for (let i = 1; i <= 12; i++) {
      const a = this.address(rnd);
      a[5] = rnd.choice(COUNTIES.slice(0, 6)); // stores sit in a few counties, all in TN
      a[6] = "TN";
      a[9] = -5;
      yield [i, bkey(i), daysOf(1997, 3, 13), null, null, syllables(i), rnd.int(200, 300), rnd.int(5_000_000, 10_000_000), "8AM-8PM",
        this.words(rnd, 2), rnd.int(1, 10), "Unknown", this.words(rnd, 6), this.words(rnd, 2), 1, "Unknown", 1, "Unknown", ...a, rnd.choice(TAX_RATES)];
    }
  }

  *callCenter(rnd: Random): Generator<Value[]> {
    const open = daysOf(1998, 1, 1);
    for (let i = 1; i <= 6; i++) {
      yield [i, bkey(i), open, null, null, dateSk(open), CALL_CENTERS[i - 1]!, rnd.choice(["small", "medium", "large"]),
        rnd.int(100, 10_000), rnd.int(10_000, 500_000), "8AM-4PM", this.words(rnd, 2), rnd.int(1, 6), this.words(rnd, 4),
        this.words(rnd, 8), this.words(rnd, 2), rnd.int(1, 6), this.words(rnd, 1), rnd.int(1, 6), this.words(rnd, 1),
        ...this.address(rnd), rnd.choice(TAX_RATES)];
    }
  }

  *webSite(rnd: Random): Generator<Value[]> {
    const open = daysOf(1997, 8, 16);
    for (let i = 1; i <= 30; i++) {
      yield [i, bkey(i), open, null, `site_${Math.floor((i - 1) / 6)}`, dateSk(open), null, "Unknown", this.words(rnd, 2),
        rnd.int(1, 6), this.words(rnd, 4), this.words(rnd, 8), this.words(rnd, 2), rnd.int(1, 6), syllables(i),
        ...this.address(rnd), rnd.choice(TAX_RATES)];
    }
  }

  *webPage(rnd: Random): Generator<Value[]> {
    const types = ["welcome", "protected", "dynamic", "feedback", "general", "order", "ad"];
    const created = daysOf(1997, 9, 3);
    for (let i = 1; i <= 60; i++) {
      yield [i, bkey(i), created, null, dateSk(created), dateSk(daysOf(2000, 1, 1)) + rnd.int(0, 100), rnd.choice(["Y", "N"]),
        rnd.random() < 0.3 ? rnd.int(1, this.counts.customer!) : null, "http://www.foo.com", rnd.choice(types),
        rnd.int(300, 8000), rnd.int(2, 25), rnd.int(1, 7), rnd.int(0, 4)];
    }
  }

  *catalogPage(rnd: Random): Generator<Value[]> {
    const first = dateSk(daysOf(1998, 1, 1));
    for (let i = 1; i <= this.counts.catalog_page!; i++) {
      const k = Math.floor(i / 108);
      yield [i, bkey(i), first + k * 30, first + k * 30 + 29, "DEPARTMENT", k + 1, (i % 108) + 1, this.words(rnd, 6),
        rnd.choice(["bi-annual", "quarterly", "monthly"])];
    }
  }

  *item(rnd: Random): Generator<Value[]> {
    const n = this.counts.item!;
    const nManufact = Math.max(100, Math.min(1000, Math.floor(n / 2)));
    // Every manufacturer and manager id is used (when there are enough items), so the
    // query parameters select rows even at small scales.
    const manufacts = this.balanced(rnd, nManufact, n), managers = this.balanced(rnd, 100, n);
    let itemId = 0;
    for (let sk = 1; sk <= n; sk++) {
      if (sk === 1 || rnd.random() < 0.6) itemId++; // ~40% of items have a second version
      const c = rnd.int(0, 9), cl = rnd.int(0, 3), brand = rnd.int(1, 10);
      const price = round(rnd.uniform(0.09, 99.99));
      const manufact = manufacts[sk - 1]!;
      const [cat, classes] = CATEGORIES[c]!;
      yield [sk, bkey(itemId), daysOf(1997, 10, 27), null, `${this.words(rnd, 8)} #${sk}`, price, round(price * rnd.uniform(0.3, 0.9)),
        (c + 1) * 1_000_000 + (cl + 1) * 1000 + brand, `${BRAND_PREFIXES[c]}${BRAND_SUFFIXES[cl]} #${brand}`, cl + 1, classes[cl]!,
        c + 1, cat, manufact, syllables(manufact), rnd.choice(SIZES), `${String(rnd.next32()).padStart(10, "0")}${String(rnd.next32()).padStart(10, "0")}`,
        rnd.choice(COLORS), rnd.choice(UNITS), "Unknown", managers[sk - 1]!, syllables(sk)];
    }
  }

  *promotion(rnd: Random): Generator<Value[]> {
    const first = dateSk(SALES_FIRST);
    for (let i = 1; i <= 300; i++) {
      const start = first + rnd.int(0, 1800);
      yield [i, bkey(i), start, start + rnd.int(30, 90), rnd.int(1, this.counts.item!), 1000, 1, syllables(i),
        ...Array.from({ length: 8 }, () => rnd.choice(["Y", "N"])), this.words(rnd, 6), "Unknown", "N"];
    }
  }

  *customer(rnd: Random): Generator<Value[]> {
    const c = this.counts, first = dateSk(SALES_FIRST);
    for (let i = 1; i <= c.customer!; i++) {
      const fn = rnd.choice(FIRST_NAMES), ln = rnd.choice(LAST_NAMES);
      const sales = first + rnd.int(0, 1800);
      yield [i, bkey(i), rnd.int(1, c.customer_demographics!), rnd.int(1, 7200), rnd.int(1, c.customer_address!), sales + rnd.int(0, 30), sales,
        rnd.choice(["Mr.", "Mrs.", "Ms.", "Dr.", "Sir", "Miss"]), fn, ln, rnd.choice(["Y", "N"]), rnd.int(1, 28), rnd.int(1, 12),
        rnd.int(1924, 1992), rnd.choice(COUNTRIES), null, `${fn}.${ln}@${rnd.choice(["example.com", "mail.test", "web.test"])}`, first + rnd.int(0, 1800)];
    }
  }

  *inventory(rnd: Random): Generator<Value[]> {
    for (const d of INVENTORY_DATES) for (let i = 1; i <= this.counts.item!; i += 10) for (let w = 1; w <= 5; w++) yield [dateSk(d), i, w, this.maybe(rnd, rnd.int(0, 1000), 0.05)];
  }

  // --- facts ------------------------------------------------------------------------

  /** (day, orders on that day) for the sales years; November and December are busier. */
  *saleDays(orders: number): Generator<[number, number]> {
    const days = Array.from({ length: SALES_LAST - SALES_FIRST + 1 }, (_, i) => SALES_FIRST + i);
    const weights = days.map((d) => (civil(d).m >= 11 ? 2 : 1));
    const total = weights.reduce((a, b) => a + b, 0);
    let acc = 0, done = 0;
    for (let i = 0; i < days.length; i++) {
      acc += weights[i]!;
      const upto = Math.round((acc / total) * orders);
      yield [days[i]!, upto - done];
      done = upto;
    }
  }

  customerKeys(rnd: Random): Value[] {
    const c = this.counts;
    return [this.maybe(rnd, rnd.int(1, c.customer!)), this.maybe(rnd, rnd.int(1, c.customer_demographics!)),
      this.maybe(rnd, rnd.int(1, 7200)), this.maybe(rnd, rnd.int(1, c.customer_address!))];
  }

  /** Return amounts for `rq` of a line's units: qty, amt, tax, amt+tax, fee, ship, cash, reversed, credit, net loss. */
  returnMoney(rnd: Random, p: number[], rq: number): Value[] {
    const amt = round(p[2]! * rq), tax = round(amt * rnd.choice(TAX_RATES));
    const cash = round(amt * rnd.random()), credit = round((amt - cash) * rnd.random());
    const fee = round(rnd.uniform(0.5, 100)), ship = round(p[0]! * rq * rnd.uniform(0, 0.5));
    return [rq, amt, tax, round(amt + tax), fee, ship, cash, round(amt - cash - credit), credit, round(fee + ship + tax)];
  }

  async storeSales(): Promise<void> {
    const rnd = this.rnd("store_sales"), nItems = this.counts.item!;
    const sales = await this.writer("store_sales"), returns: Value[][] = [];
    let ticket = 0;
    for (const [day, tickets] of this.saleDays(Math.round(this.counts.store_sales! / 12))) {
      const sk = dateSk(day);
      for (let t = 0; t < tickets; t++) {
        ticket++;
        const dateKey = this.maybe(rnd, sk, 0.01), timeKey = this.maybe(rnd, rnd.int(8 * 3600, 22 * 3600 - 1));
        const [cust, cdemo, hdemo, addr] = this.customerKeys(rnd);
        const store = this.maybe(rnd, rnd.int(1, 12));
        for (const item of rnd.sample(1, nItems, rnd.int(8, 16))) {
          const q = rnd.int(1, 100), p = this.prices(rnd, q);
          const net = round(p[4]! - p[8]!);
          await sales.add([dateKey, timeKey, item, cust!, cdemo!, hdemo!, addr!, store, this.maybe(rnd, rnd.int(1, 300)), ticket, q, ...p, net, round(net + p[7]!), round(net - p[5]!)]);
          if (rnd.random() < 0.1) {
            const when = [sk + rnd.int(1, 90), rnd.int(8 * 3600, 22 * 3600 - 1)];
            const m = this.returnMoney(rnd, p, rnd.int(1, q));
            returns.push([...when, item, cust!, cdemo!, hdemo!, addr!, store, rnd.int(1, 35), ticket, ...m]);
          }
        }
      }
    }
    this.counts.store_sales = await sales.flush();
    this.report("store_sales", this.counts.store_sales, this.counts.store_sales);
    await this.returns("store_returns", returns);
  }

  async returns(table: string, rows: Value[][]): Promise<void> {
    this.counts[table] = rows.length;
    rows.sort((a, b) => (a[0] as number) - (b[0] as number));
    await this.simple(table, rows);
  }

  /** catalog_sales and web_sales: orders of 4-14 lines, shipped 1-130 days later. */
  async channelSales(table: string, retTable: string): Promise<void> {
    const rnd = this.rnd(table), c = this.counts, nItems = c.item!, web = table === "web_sales";
    const sales = await this.writer(table), returns: Value[][] = [];
    let order = 0;
    for (const [day, orders] of this.saleDays(Math.round(c[table]! / 9))) {
      const sk = dateSk(day);
      for (let o = 0; o < orders; o++) {
        order++;
        const dateKey = this.maybe(rnd, sk, 0.01), timeKey = this.maybe(rnd, rnd.int(0, 86_399));
        const bill = this.customerKeys(rnd);
        const shipTo = rnd.random() < 0.9 ? bill : this.customerKeys(rnd);
        const place = web ? [this.maybe(rnd, rnd.int(1, 60)), this.maybe(rnd, rnd.int(1, 30))]
          : [this.maybe(rnd, rnd.int(1, 6)), this.maybe(rnd, rnd.int(1, c.catalog_page!))];
        const mode = this.maybe(rnd, rnd.int(1, 20)), wh = this.maybe(rnd, rnd.int(1, 5));
        const shipDate = this.maybe(rnd, sk + rnd.int(1, 130));
        for (const item of rnd.sample(1, nItems, rnd.int(4, 14))) {
          const q = rnd.int(1, 100), p = this.prices(rnd, q);
          const shipCost = round(p[0]! * q * rnd.uniform(0, 0.5));
          const net = round(p[4]! - p[8]!);
          const promo = this.maybe(rnd, rnd.int(1, 300));
          const measures = [order, q, ...p, shipCost, net, round(net + p[7]!), round(net + shipCost), round(net + shipCost + p[7]!), round(net - p[5]!)];
          await sales.add(web
            ? [dateKey, timeKey, shipDate, item, ...bill, ...shipTo, ...place, mode, wh, promo, ...measures]
            : [dateKey, timeKey, shipDate, ...bill, ...shipTo, ...place, mode, wh, item, promo, ...measures]);
          if (rnd.random() < 0.1) {
            const rq = rnd.int(1, q);
            const when = [sk + rnd.int(1, 90), rnd.int(0, 86_399), item];
            const money = this.returnMoney(rnd, p, rq);
            const reason = rnd.int(1, 35);
            returns.push(web ? [...when, ...bill, ...shipTo, place[0]!, reason, order, ...money]
              : [...when, ...bill, ...shipTo, ...place, mode, wh, reason, order, ...money]);
          }
        }
      }
    }
    c[table] = await sales.flush();
    this.report(table, c[table]!, c[table]!);
    await this.returns(retTable, returns);
  }

  async table(t: string): Promise<void> {
    const r = this.rnd(t);
    switch (t) {
      case "date_dim": return this.simple(t, this.dateDim());
      case "time_dim": return this.simple(t, this.timeDim());
      case "income_band": return this.simple(t, Array.from({ length: 20 }, (_, i) => [i + 1, i * 10_000 + (i ? 1 : 0), (i + 1) * 10_000]));
      case "household_demographics": return this.simple(t, this.householdDemographics());
      case "customer_demographics": return this.simple(t, this.customerDemographics());
      case "reason": return this.simple(t, Array.from({ length: 35 }, (_, i) => [i + 1, bkey(i + 1), `reason ${i + 1}`]));
      case "ship_mode": {
        const types = ["EXPRESS", "NEXT DAY", "OVERNIGHT", "TWO DAY", "LIBRARY", "REGULAR"];
        const codes = ["AIR", "SURFACE", "SEA", "BIKE", "HAND", "MESSENGER"];
        return this.simple(t, Array.from({ length: 20 }, (_, k) => {
          const i = k + 1;
          return [i, bkey(i), types[(i - 1) % 6]!, codes[Math.floor((i - 1) / 4) % 6]!, CARRIERS[i - 1]!, r.hex(10)];
        }));
      }
      case "warehouse": return this.simple(t, Array.from({ length: 5 }, (_, k) => [k + 1, bkey(k + 1), this.words(r, 3).slice(0, 20), r.int(50_000, 1_000_000), ...this.address(r)]));
      case "store": return this.simple(t, this.store(r));
      case "call_center": return this.simple(t, this.callCenter(r));
      case "web_site": return this.simple(t, this.webSite(r));
      case "web_page": return this.simple(t, this.webPage(r));
      case "catalog_page": return this.simple(t, this.catalogPage(r));
      case "item": return this.simple(t, this.item(r));
      case "promotion": return this.simple(t, this.promotion(r));
      case "customer_address": {
        const types = ["apartment", "condo", "single family"];
        return this.simple(t, Array.from({ length: this.counts.customer_address! }, (_, k) => [k + 1, bkey(k + 1), ...this.address(r), r.choice(types)]));
      }
      case "customer": return this.simple(t, this.customer(r));
      case "inventory": return this.simple(t, this.inventory(r));
      case "store_sales": return this.storeSales();
      case "catalog_sales": return this.channelSales("catalog_sales", "catalog_returns");
      case "web_sales": return this.channelSales("web_sales", "web_returns");
    }
  }
}

/** Creates and fills the TPC-DS tables, replacing any with the same names. Returns
 * the row counts. */
export async function generateTpcds(open: OpenTable, scale = 0.1, o: TpcdsOptions = {}): Promise<Record<string, number>> {
  if (!(scale > 0 && scale <= 1)) throw new RangeError("scale must be in (0, 1]");
  const g = new Gen(open, scale, o);
  for (const t of generationOrder()) {
    if (t.endsWith("_returns")) continue; // written together with its sales table
    const t0 = performance.now();
    g.report(t, 0, g.counts[t]!);
    await g.table(t);
    for (const done of t.endsWith("_sales") ? [t, t.replace("_sales", "_returns")] : [t]) {
      o.log?.(`${done}: ${g.counts[done]!.toLocaleString("en-US")} rows, ${SCHEMA[done]!.length} columns (${((performance.now() - t0) / 1000).toFixed(1)} s)`);
    }
  }
  return g.counts;
}
