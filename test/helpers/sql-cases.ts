// The cases the engine is checked on against SQLite: three small random tables with
// nulls and duplicate keys, queries whose rows come back sorted (by a unique key) and
// queries compared as multisets, and the cluster configurations to run them on.
import type { Value } from "../../src/core/types.ts";
import type { Database } from "../../src/api/database.ts";
import { Random } from "../../src/loaders/random.ts";
import { tables } from "./db.ts";

const rnd = new Random("sql-vs-sqlite");
const maybe = <T,>(v: T, p = 0.1): T | null => (rnd.random() < p ? null : v);
const users: Value[][] = Array.from({ length: 60 }, (_, i) => [i + 1, maybe(rnd.choice(["ann", "bob", "cid", "dan", "eve"])), maybe(rnd.choice(["PL", "DE", "US"])), maybe(rnd.int(18, 80))]);
const orders: Value[][] = Array.from({ length: 400 }, (_, i) => [i + 1, maybe(rnd.int(1, 70)), maybe(Math.round(rnd.uniform(1, 500) * 100) / 100), maybe(rnd.choice(["new", "paid", "shipped"])), maybe(rnd.int(0, 9))]);
const items: Value[][] = Array.from({ length: 10 }, (_, i) => [i, `item${i}`, maybe(rnd.int(1, 5))]);

export const SORTED = [
  "select id, name from users where age > 40 order by id",
  "select id, amount from orders where amount between 100 and 200 and status <> 'new' order by id",
  "select status, count(*) n, sum(amount) s, min(amount) mn, max(amount) mx from orders group by status order by status nulls first",
  "select u.id, count(o.id) n from users u left join orders o on o.user_id = u.id group by u.id order by u.id",
  "select country, count(*) from users group by country having count(*) > 5 order by country nulls last",
  "select user_id, avg(amount) a from orders group by 1 having a > 200 order by user_id nulls first",
  "select country c, count(*) n from users group by c having n > 5 and c <> 'DE' order by c nulls first",
  "select status, sum(amount) total from orders group by status having total > 1000 and count(*) > 10 order by status nulls first",
  "select o.id oid, u.name uname from orders o join users u on o.user_id = u.id where u.country = 'PL' order by o.id desc limit 15",
  "select id from orders where status in ('new', 'paid') and item is not null order by amount desc nulls last, id limit 7",
  "select distinct status from orders order by status nulls first",
  "with big as (select user_id, sum(amount) total from orders group by user_id) select u.id, b.total from users u join big b on b.user_id = u.id where b.total > 1000 order by u.id",
  "select o.id oid, i.name iname from orders o left join items i on o.item = i.id where o.id < 50 order by o.id",
  "select u.id uid, o.id oid from users u right join orders o on o.user_id = u.id where o.id < 40 order by o.id",
  "select x.status, x.n from (select status, count(*) n from orders where amount > 50 group by status) x where x.n > 10 order by x.status nulls first",
  "select id, case when amount > 250 then 'high' when amount > 100 then 'mid' else 'low' end from orders order by id limit 30",
  "select u.name, sum(o.amount) from users u, orders o, items i where o.user_id = u.id and o.item = i.id and i.weight > 2 group by u.name order by u.name nulls first",
  "select count(*), count(age), sum(age), min(name), max(name) from users",
  "select count(*) from orders where user_id not in (1, 2, 3)",
  "select id, coalesce(name, 'none') from users where id <= 10 order by id",
  "select status, count(distinct_users) from (select distinct status, user_id distinct_users from orders where user_id is not null) d group by status order by status nulls first",
  "select a.id aid, b.id bid from users a join users b on a.age = b.age and a.id < b.id where a.id < 20 order by a.id, b.id",
];
export const UNSORTED = [
  "select * from orders where amount > 400",
  "select o.status, u.country from orders o join users u on u.id = o.user_id",
  "select user_id, item, count(*) from orders group by user_id, item",
  "select u.id uid, i.name iname from users u cross join items i where u.id < 4",
  "select * from users where name like '%a%' or age < 25",
];

/** The tables, in a Database with 37 rows per data file. */
export function sqlCaseTables(): Promise<Database> {
  return tables({
    users: { columns: [["id", "int"], ["name", "string"], ["country", "string"], ["age", "int"]], rows: users },
    orders: { columns: [["id", "int"], ["user_id", "int"], ["amount", "float"], ["status", "string"], ["item", "int"]], rows: orders },
    items: { columns: [["id", "int"], ["name", "string"], ["weight", "int"]], rows: items },
  }, 37);
}

/** The configurations every query runs on. */
export const CONFIGS = [
  { cluster: { workers: 1 } },
  { cluster: { workers: 3, batchRows: 16, splitRows: 20 }, planner: { joinStrategy: "broadcast" as const, shufflePartitions: 3 } },
  { cluster: { workers: 4, batchRows: 8, splitRows: 10 }, planner: { joinStrategy: "shuffle" as const, shufflePartitions: 5 } },
  { cluster: { workers: 2 }, rules: [] as string[] },
];
