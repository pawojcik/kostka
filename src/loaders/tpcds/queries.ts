// Example queries over the TPC-DS tables. Each follows the TPC-DS query with the same
// number, with one set of substitution parameters. `sql` is kostka SQL; `reference`,
// where kostka's needs a different shape, is the original form, which the tests run
// on SQLite over the same data to check the results.

export interface ExampleQuery {
  id: string;
  title: string;
  about: string;
  sql: string;
  reference?: string;
}

export const QUERIES: ExampleQuery[] = [
  {
    id: "q3",
    title: "Q3 · November brand sales of one manufacturer",
    about: "A star join: store_sales with two dimensions, filtered on both, grouped and ordered. The filters on date_dim and item are pushed below the joins, so the hash tables hold only the matching days and items.",
    sql: `select d_year, i_brand_id, i_brand, sum(ss_ext_sales_price) sum_agg
from date_dim, store_sales, item
where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk
  and i_manufact_id = 28 and d_moy = 11
group by d_year, i_brand, i_brand_id
order by d_year, sum_agg desc, i_brand_id
limit 100`,
  },
  {
    id: "q55",
    title: "Q55 · brand revenue for one manager and month",
    about: "The simplest star join. Watch the scan of date_dim: 73,049 days in files sorted by date, and file pruning reads only the file holding 1999.",
    sql: `select i_brand_id, i_brand, sum(ss_ext_sales_price) ext_price
from date_dim, store_sales, item
where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk
  and i_manager_id = 28 and d_moy = 11 and d_year = 1999
group by i_brand, i_brand_id
order by ext_price desc, i_brand_id
limit 100`,
  },
  {
    id: "q7",
    title: "Q7 · average sale for one customer segment",
    about: "Five tables. The condition on promotion is an OR of two columns of the same table, so it is still pushed down to that scan. Note how few customer_demographics rows survive the filter: the build side is tiny.",
    sql: `select i_item_id, avg(ss_quantity) agg1, avg(ss_list_price) agg2, avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
from store_sales, customer_demographics, date_dim, item, promotion
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_cdemo_sk = cd_demo_sk
  and ss_promo_sk = p_promo_sk and cd_gender = 'M' and cd_marital_status = 'S'
  and cd_education_status = 'College' and (p_channel_email = 'N' or p_channel_event = 'N')
  and d_year = 2000
group by i_item_id
order by i_item_id
limit 100`,
  },
  {
    id: "q19",
    title: "Q19 · brands bought away from home",
    about: "Six tables. The last condition compares the customer's zip code with the store's, so it needs columns from both sides: it can only run after the store join, as a filter above it.",
    sql: `select i_brand, i_brand_id, i_manufact_id, i_manufact, sum(ss_ext_sales_price) ext_price
from date_dim, store_sales, item, customer, customer_address, store
where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk and i_manager_id = 8
  and d_moy = 11 and d_year = 1998 and ss_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
  and ss_store_sk = s_store_sk
group by i_brand, i_brand_id, i_manufact_id, i_manufact
order by ext_price desc, i_brand, i_brand_id, i_manufact_id, i_manufact
limit 100`,
  },
  {
    id: "q96",
    title: "Q96 · evening sales count",
    about: "count(*) over a four-way join. Every scan reads only the columns needed for the joins and filters.",
    sql: `select count(*) count
from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = t_time_sk and ss_hdemo_sk = hd_demo_sk and ss_store_sk = s_store_sk
  and t_hour = 20 and t_minute >= 30 and hd_dep_count = 7 and s_store_name = 'ese'
`,
  },
  {
    id: "q43",
    title: "Q43 · sales per weekday and store",
    about: "A pivot: one sum per weekday. SQL writes each as SUM(CASE WHEN day = ... THEN price END); here the comparison is cast to 0/1 and multiplied.",
    sql: `select s_store_name, s_store_id,
  sum(case when d_day_name = 'Sunday' then ss_sales_price else null end) sun_sales,
  sum(case when d_day_name = 'Monday' then ss_sales_price else null end) mon_sales,
  sum(case when d_day_name = 'Tuesday' then ss_sales_price else null end) tue_sales,
  sum(case when d_day_name = 'Wednesday' then ss_sales_price else null end) wed_sales,
  sum(case when d_day_name = 'Thursday' then ss_sales_price else null end) thu_sales,
  sum(case when d_day_name = 'Friday' then ss_sales_price else null end) fri_sales,
  sum(case when d_day_name = 'Saturday' then ss_sales_price else null end) sat_sales
from date_dim, store_sales, store
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and s_gmt_offset = -5
  and d_year = 2000
group by s_store_name, s_store_id
order by s_store_name, s_store_id
limit 100`,
  },
  {
    id: "q34",
    title: "Q34 · customers with big shopping baskets",
    about: "Two aggregation levels: first count the lines of each ticket, then keep tickets with 15 to 20 lines and join the customer. Three pipelines are cut at hash builds, then one at the aggregate, one at the sort.",
    sql: `select c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt
from (select ss_ticket_number, ss_customer_sk, count(*) cnt
      from store_sales, date_dim, store, household_demographics
      where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk
        and ss_hdemo_sk = hd_demo_sk
        and (d_dom between 1 and 3 or d_dom between 25 and 28)
        and (hd_buy_potential = '>10000' or hd_buy_potential = 'Unknown')
        and hd_vehicle_count > 0
        and (case when hd_vehicle_count > 0
             then 1.0 * hd_dep_count / hd_vehicle_count else null end) > 1.2
        and d_year in (1999, 2000, 2001)
        and s_county in ('Williamson County', 'Franklin Parish', 'Bronx County',
                         'Orange County')
      group by ss_ticket_number, ss_customer_sk) dn, customer
where ss_customer_sk = c_customer_sk and cnt between 15 and 20
order by c_last_name, c_first_name, c_salutation, c_preferred_cust_flag desc,
         ss_ticket_number`,
  },
  {
    id: "q15",
    title: "Q15 · catalog sales by zip code",
    about: "The OR in the filter mixes customer_address and catalog_sales columns, so unlike the other conditions it cannot be pushed into either scan. kostka does not reorder joins: date_dim is joined first because it removes the most rows (in SQL's FROM order it is last, and the query is 5x slower).",
    sql: `select ca_zip, sum(cs_sales_price) total
from catalog_sales, customer, customer_address, date_dim
where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk
  and (substr(ca_zip, 1, 5) in ('85669', '86197', '88274', '83405', '86475', '85392',
                                '85460', '80348', '81792')
       or ca_state in ('CA', 'WA', 'GA') or cs_sales_price > 500)
  and cs_sold_date_sk = d_date_sk and d_qoy = 2 and d_year = 2001
group by ca_zip
order by ca_zip
limit 100`,
  },
  {
    id: "q99",
    title: "Q99 · catalog shipping delays",
    about: "Buckets of days between sale and shipping, per warehouse, ship mode and call center. Grouping by an expression (the first 20 characters of the warehouse name) works like grouping by a column.",
    sql: `select substr(w_warehouse_name, 1, 20) warehouse, sm_type, cc_name,
  sum(case when cs_ship_date_sk - cs_sold_date_sk <= 30 then 1 else 0 end) d30,
  sum(case when cs_ship_date_sk - cs_sold_date_sk > 30
            and cs_ship_date_sk - cs_sold_date_sk <= 60 then 1 else 0 end) d31_60,
  sum(case when cs_ship_date_sk - cs_sold_date_sk > 60
            and cs_ship_date_sk - cs_sold_date_sk <= 90 then 1 else 0 end) d61_90,
  sum(case when cs_ship_date_sk - cs_sold_date_sk > 90
            and cs_ship_date_sk - cs_sold_date_sk <= 120 then 1 else 0 end) d91_120,
  sum(case when cs_ship_date_sk - cs_sold_date_sk > 120 then 1 else 0 end) d120_plus
from catalog_sales, warehouse, ship_mode, call_center, date_dim
where d_month_seq between 1200 and 1211 and cs_ship_date_sk = d_date_sk
  and cs_warehouse_sk = w_warehouse_sk and cs_ship_mode_sk = sm_ship_mode_sk
  and cs_call_center_sk = cc_call_center_sk
group by substr(w_warehouse_name, 1, 20), sm_type, cc_name
order by warehouse, sm_type, cc_name
limit 100`,
  },
  {
    id: "q1",
    title: "Q1 · customers who return a lot",
    about: "A CTE used twice and a correlated subquery, rewritten as two copies of the same aggregate joined on the store: one per customer and store, one averaged per store. kostka has no correlated subqueries, so the per-store average is a second CTE joined in.",
    sql: `with customer_total_return as (
  select sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk,
         sum(sr_return_amt) as ctr_total_return
  from store_returns, date_dim
  where sr_returned_date_sk = d_date_sk and d_year = 2000
  group by sr_customer_sk, sr_store_sk),
store_avg as (
  select ctr_store_sk as avg_store_sk, avg(ctr_total_return) * 1.2 as limit_return
  from customer_total_return
  group by ctr_store_sk)
select c_customer_id
from customer_total_return ctr1, store_avg, store, customer
where ctr1.ctr_store_sk = avg_store_sk and ctr1.ctr_total_return > limit_return
  and s_store_sk = ctr1.ctr_store_sk and s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100`,
    reference: `with customer_total_return as (
  select sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk,
         sum(sr_return_amt) as ctr_total_return
  from store_returns, date_dim
  where sr_returned_date_sk = d_date_sk and d_year = 2000
  group by sr_customer_sk, sr_store_sk)
select c_customer_id
from customer_total_return ctr1, store, customer
where ctr1.ctr_total_return > (select avg(ctr_total_return) * 1.2
                               from customer_total_return ctr2
                               where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk and s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100`,
  },
  {
    id: "q65",
    title: "Q65 · items selling far below their store's average",
    about: "Revenue per store and item, compared with the store's average item revenue. The same aggregate is built twice, so the program has two independent pipelines scanning store_sales.",
    sql: `select s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand
from store, item,
  (select ss_store_sk, avg(revenue) as ave
   from (select ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
         from store_sales, date_dim
         where ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1187
         group by ss_store_sk, ss_item_sk) sa
   group by ss_store_sk) sb,
  (select ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
   from store_sales, date_dim
   where ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1187
   group by ss_store_sk, ss_item_sk) sc
where sb.ss_store_sk = sc.ss_store_sk and sc.revenue <= 0.1 * sb.ave
  and s_store_sk = sc.ss_store_sk and i_item_sk = sc.ss_item_sk
order by s_store_name, i_item_desc
limit 100`,
  },
  {
    id: "q93",
    title: "Q93 · sales net of returns for one reason",
    about: "A left join of store_sales with store_returns on two keys (item and ticket). The hash table is built on the returns; the inner join with reason then keeps only rows that had a return.",
    sql: `select ss_customer_sk, sum(act_sales) sumsales
from (select ss_item_sk, ss_ticket_number, ss_customer_sk,
        case when sr_return_quantity is not null
             then (ss_quantity - sr_return_quantity) * ss_sales_price
             else ss_quantity * ss_sales_price end act_sales
      from store_sales left outer join store_returns
        on (sr_item_sk = ss_item_sk and sr_ticket_number = ss_ticket_number), reason
      where sr_reason_sk = r_reason_sk and r_reason_desc = 'reason 28') t
group by ss_customer_sk
order by sumsales, ss_customer_sk
limit 100`,
  },
  {
    id: "x1",
    title: "X1 · customers buying the same items in store and catalog (shuffle-heavy)",
    about: "Not a TPC-DS query: built to parallelize and shuffle a lot. Three scans that don't depend on each other run at the same time. Grouping store and catalog sales by customer and item makes about one group per row, so the partial groups shuffle nearly all of both tables. The two aggregates are then joined on both keys, too big to broadcast, so both sides are shuffled again. On four workers it shuffles about 32 MB at scale 0.1.",
    sql: `with store as (
  select ss_customer_sk s_cust, ss_item_sk s_item, sum(ss_net_paid) store_paid
  from store_sales group by ss_customer_sk, ss_item_sk),
catalog as (
  select cs_bill_customer_sk cat_cust, cs_item_sk cat_item, sum(cs_net_paid) catalog_paid
  from catalog_sales group by cs_bill_customer_sk, cs_item_sk),
web as (
  select ws_bill_customer_sk web_cust, sum(ws_net_paid) web_paid, count(*) web_orders
  from web_sales group by ws_bill_customer_sk),
both_channels as (
  select s_cust, count(*) items_both, sum(store_paid + catalog_paid) paid_both
  from store join catalog on s_cust = cat_cust and s_item = cat_item
  group by s_cust)
select c_customer_id, c_last_name, items_both, paid_both, web_orders, web_paid
from both_channels left join web on s_cust = web_cust
  join customer on s_cust = c_customer_sk
order by items_both desc, paid_both desc, c_customer_id
limit 100`,
  },
];
