// Example queries over the users/orders sample (Database.loadSample): 2,000 users in a
// dozen cities and 20,000 orders. "Revenue per city" joins every user, and "orders
// from one city" only those of one city: the Spark-style engine shuffles the first
// join and broadcasts the second (ADR 0002).

import type { ExampleQuery } from "../tpcds/queries.ts";

export const QUERIES: ExampleQuery[] = [
  {
    id: "s1", title: "Sample · revenue per city", about: "A join and an aggregate over the users/orders sample.",
    sql: "select u.city, count(*) orders, round(sum(o.amount), 2) revenue\nfrom orders o join users u on o.user_id = u.id\nwhere o.status <> 'returned'\ngroup by u.city\norder by revenue desc",
  },
  {
    id: "s2", title: "Sample · top orders with a CTE", about: "A CTE, CASE and a top-N.",
    sql: "with big as (\n  select user_id, sum(amount) total from orders group by user_id\n)\nselect u.name, b.total,\n  case when b.total > 2000 then 'gold' when b.total > 1000 then 'silver' else 'bronze' end tier\nfrom big b join users u on u.id = b.user_id\norder by b.total desc\nlimit 10",
  },
  {
    id: "s3", title: "Sample · orders from one city", about: "The same join, but only the users of one city: a small build side, which the Spark-style engine broadcasts.",
    sql: "select u.name, count(*) orders\nfrom orders o join users u on o.user_id = u.id\nwhere u.city = 'Gdansk'\ngroup by u.name",
  },
];
