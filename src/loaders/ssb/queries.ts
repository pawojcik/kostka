// The 13 Star Schema Benchmark queries, in four flights. Every query joins the lineorder
// fact table with some of its dimensions; within a flight the queries ask the same
// question with ever narrower filters, so each one reads fewer rows than the last.

import type { ExampleQuery } from "../tpcds/queries.ts";

export const QUERIES: ExampleQuery[] = [
  {
    id: "ssb1.1",
    title: "SSB Q1.1 · revenue from small discounts in 1993",
    about: "Flight 1 joins lineorder with only the date dimension. How much more would revenue have been without the 1-3% discounts on small orders in 1993? The filter on d_year shrinks date to 365 rows, and lineorder is in date order, so file pruning skips most of its files.",
    sql: `select sum(lo_extendedprice * lo_discount) revenue
from lineorder, date
where lo_orderdate = d_datekey
  and d_year = 1993
  and lo_discount between 1 and 3
  and lo_quantity < 25`,
  },
  {
    id: "ssb1.2",
    title: "SSB Q1.2 · the same for one month",
    about: "Q1.1 narrowed to January 1994 and larger quantities. Compare the rows scanned with Q1.1: the date filter now keeps 31 days.",
    sql: `select sum(lo_extendedprice * lo_discount) revenue
from lineorder, date
where lo_orderdate = d_datekey
  and d_yearmonthnum = 199401
  and lo_discount between 4 and 6
  and lo_quantity between 26 and 35`,
  },
  {
    id: "ssb1.3",
    title: "SSB Q1.3 · the same for one week",
    about: "Narrowed to week 6 of 1994: seven days. The hash table built from date holds just those seven keys.",
    sql: `select sum(lo_extendedprice * lo_discount) revenue
from lineorder, date
where lo_orderdate = d_datekey
  and d_weeknuminyear = 6 and d_year = 1994
  and lo_discount between 5 and 7
  and lo_quantity between 26 and 35`,
  },
  {
    id: "ssb2.1",
    title: "SSB Q2.1 · yearly revenue per brand, one category, American suppliers",
    about: "Flight 2 joins three dimensions and groups the result. The filters on part and supplier run on the dimension scans, below the joins, so only a fifth of the suppliers and a 25th of the parts reach the hash tables.",
    sql: `select sum(lo_revenue) revenue, d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and p_category = 'MFGR#12'
  and s_region = 'AMERICA'
group by d_year, p_brand1
order by d_year, p_brand1`,
  },
  {
    id: "ssb2.2",
    title: "SSB Q2.2 · eight brands, Asian suppliers",
    about: "A range of brands instead of a category: a BETWEEN on strings. The part side shrinks to 8 of 1,000 brands.",
    sql: `select sum(lo_revenue) revenue, d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and p_brand1 between 'MFGR#2221' and 'MFGR#2228'
  and s_region = 'ASIA'
group by d_year, p_brand1
order by d_year, p_brand1`,
  },
  {
    id: "ssb2.3",
    title: "SSB Q2.3 · one brand, European suppliers",
    about: "One brand. With so few matching parts, a runtime filter built from the part join can drop most lineorder rows right at the scan.",
    sql: `select sum(lo_revenue) revenue, d_year, p_brand1
from lineorder, date, part, supplier
where lo_orderdate = d_datekey
  and lo_partkey = p_partkey
  and lo_suppkey = s_suppkey
  and p_brand1 = 'MFGR#2239'
  and s_region = 'EUROPE'
group by d_year, p_brand1
order by d_year, p_brand1`,
  },
  {
    id: "ssb3.1",
    title: "SSB Q3.1 · revenue between Asian nations",
    about: "Flight 3 asks how much customers in one place bought from suppliers in another, per year. Four tables, both sides of the trade filtered on region, grouped by the two nations.",
    sql: `select c_nation, s_nation, d_year, sum(lo_revenue) revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_region = 'ASIA' and s_region = 'ASIA'
  and d_year >= 1992 and d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, revenue desc`,
  },
  {
    id: "ssb3.2",
    title: "SSB Q3.2 · revenue between US cities",
    about: "The same question within one nation, grouped by city.",
    sql: `select c_city, s_city, d_year, sum(lo_revenue) revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
  and d_year >= 1992 and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, revenue desc`,
  },
  {
    id: "ssb3.3",
    title: "SSB Q3.3 · revenue between two UK cities",
    about: "Two cities on each side, written as ORs. The ORs touch one table each, so they are still pushed down to the customer and supplier scans.",
    sql: `select c_city, s_city, d_year, sum(lo_revenue) revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
  and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
  and d_year >= 1992 and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, revenue desc`,
  },
  {
    id: "ssb3.4",
    title: "SSB Q3.4 · the same in one month",
    about: "Q3.3 for December 1997 only. This is the most selective query in SSB: at small scales it may well find no rows at all.",
    sql: `select c_city, s_city, d_year, sum(lo_revenue) revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')
  and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')
  and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc`,
  },
  {
    id: "ssb4.1",
    title: "SSB Q4.1 · profit per year and customer nation",
    about: "Flight 4 joins all five tables and sums profit, revenue minus supply cost. Americas only, two manufacturers. Watch the join order the optimizer picks: the smallest filtered dimensions are built first.",
    sql: `select d_year, c_nation, sum(lo_revenue - lo_supplycost) profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_region = 'AMERICA'
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation`,
  },
  {
    id: "ssb4.2",
    title: "SSB Q4.2 · profit per supplier nation and category, 1997-98",
    about: "Q4.1 drilled down: two years, grouped by supplier nation and part category.",
    sql: `select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_region = 'AMERICA'
  and (d_year = 1997 or d_year = 1998)
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, s_nation, p_category
order by d_year, s_nation, p_category`,
  },
  {
    id: "ssb4.3",
    title: "SSB Q4.3 · profit per US city and brand, 1997-98",
    about: "Drilled down again: US suppliers, one category, grouped by supplier city and brand.",
    sql: `select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) profit
from date, customer, supplier, part, lineorder
where lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_nation = 'UNITED STATES'
  and (d_year = 1997 or d_year = 1998)
  and p_category = 'MFGR#14'
group by d_year, s_city, p_brand1
order by d_year, s_city, p_brand1`,
  },
];
