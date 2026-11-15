// Example queries over the NYC taxi sample: trips and zones. `reference` is the same
// query in SQLite's dialect (timestamps are stored there as microseconds), which the
// tests run on the same rows to check the results.

import type { ExampleQuery } from "../tpcds/queries.ts";

const hour = (c: string) => `cast(strftime('%H', ${c} / 1000000, 'unixepoch') as int)`;
const dow = (c: string) => `((cast(strftime('%w', ${c} / 1000000, 'unixepoch') as int) + 6) % 7 + 1)`;

export const QUERIES: ExampleQuery[] = [
  {
    id: "taxi1",
    title: "Taxi · trips per hour of the day",
    about: "One table, one aggregate: when do New Yorkers take taxis? The evening peak is around 6 pm, and the few trips at 4-5 am are the longest (many go to the airports). The scan reads only the two columns the query uses, out of eleven.",
    sql: `select hour(pickup_time) hour, count(*) trips, round(avg(distance), 2) avg_miles
from trips
group by hour(pickup_time)
order by hour`,
    reference: `select ${hour("pickup_time")} hour, count(*) trips, round(avg(distance), 2) avg_miles
from trips group by 1 order by hour`,
  },
  {
    id: "taxi2",
    title: "Taxi · tips by payment type",
    about: "Cash tips are not recorded, so the cash average is near zero: the data only knows what the meter knows. Five groups, so the partial aggregates the workers send are tiny.",
    sql: `select payment, count(*) trips, round(avg(tip), 2) avg_tip,
       round(sum(tip) / sum(fare) * 100, 1) tip_percent
from trips
where fare > 0
group by payment
order by trips desc`,
  },
  {
    id: "taxi3",
    title: "Taxi · the busiest pickup zones",
    about: "A join with the zone names, then a top 10. The zones table is small (265 rows), so it is broadcast: every worker gets the whole hash table and no trip moves between workers.",
    sql: `select z.zone, z.borough, count(*) trips
from trips t join zones z on t.pickup_zone = z.zone_id
group by z.zone, z.borough
order by trips desc, z.zone
limit 10`,
  },
  {
    id: "taxi4",
    title: "Taxi · where airport trips go",
    about: "zones is joined twice, once for the pickup and once for the dropoff, each with its own alias. The filter on the airport names runs on the first zones scan, below its join.",
    sql: `select p.zone airport, d.borough destination, count(*) trips,
       round(avg(t.total), 2) avg_total, round(avg(t.distance), 1) avg_miles
from trips t
  join zones p on t.pickup_zone = p.zone_id
  join zones d on t.dropoff_zone = d.zone_id
where p.zone in ('JFK Airport', 'LaGuardia Airport')
group by p.zone, d.borough
order by airport, trips desc`,
  },
  {
    id: "taxi5",
    title: "Taxi · borough to borough",
    about: "The same two joins without a filter: how many trips stay in their borough? Most start and end in Manhattan.",
    sql: `select p.borough pickup, d.borough dropoff, count(*) trips
from trips t
  join zones p on t.pickup_zone = p.zone_id
  join zones d on t.dropoff_zone = d.zone_id
group by p.borough, d.borough
order by trips desc, pickup, dropoff
limit 15`,
  },
  {
    id: "taxi6",
    title: "Taxi · speed by day of the week",
    about: "Average trip time and speed per weekday (1 = Monday), from the pickup and dropoff times. Tuesday to Friday traffic is the slowest and Sunday the fastest; Monday is fast too, as two of this month's Mondays were holidays.",
    sql: `select day_of_week(pickup_time) weekday, count(*) trips,
       round(avg(minutes_between(pickup_time, dropoff_time)), 1) avg_minutes,
       round(sum(distance) / sum(minutes_between(pickup_time, dropoff_time)) * 60, 1) avg_mph
from trips
where dropoff_time > pickup_time
group by day_of_week(pickup_time)
order by weekday`,
    reference: `select ${dow("pickup_time")} weekday, count(*) trips,
       round(avg((dropoff_time - pickup_time) / 60000000.0), 1) avg_minutes,
       round(sum(distance) / sum((dropoff_time - pickup_time) / 60000000.0) * 60, 1) avg_mph
from trips where dropoff_time > pickup_time group by 1 order by weekday`,
  },
  {
    id: "taxi7",
    title: "Taxi · revenue per day in January",
    about: "Trips grouped by date. The files are in pickup order, so each one covers a few days of the month.",
    sql: `select cast(pickup_time as date) day, count(*) trips, round(sum(total), 2) revenue
from trips
group by cast(pickup_time as date)
order by day`,
    reference: `select cast(pickup_time / 86400000000 as int) day, count(*) trips, round(sum(total), 2) revenue
from trips group by 1 order by day`,
  },
  {
    id: "taxi8",
    title: "Taxi · one weekend in midtown",
    about: "A time range and a zone. The files are in pickup order, so file pruning skips every file outside the weekend: compare the files read with the files in the table.",
    sql: `select z.zone, hour(t.pickup_time) hour, count(*) trips, round(avg(t.tip), 2) avg_tip
from trips t join zones z on t.pickup_zone = z.zone_id
where t.pickup_time >= timestamp '2024-01-20 00:00:00'
  and t.pickup_time < timestamp '2024-01-22 00:00:00'
  and z.zone like 'Midtown%'
group by z.zone, hour(t.pickup_time)
order by z.zone, hour`,
    reference: `select z.zone, ${hour("t.pickup_time")} hour, count(*) trips, round(avg(t.tip), 2) avg_tip
from trips t join zones z on t.pickup_zone = z.zone_id
where t.pickup_time >= 1705708800000000 and t.pickup_time < 1705881600000000 and z.zone like 'Midtown%'
group by 1, 2 order by z.zone, hour`,
  },
];
