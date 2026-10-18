"""Step 6 - Serving layer in Apache Cassandra (NoSQL, wide-column).

Cassandra tables are designed query-first: each table answers one application query with a single
partition read. Spark computes the aggregates and every Spark partition writes its rows in parallel
(foreachPartition), so the load scales with the cluster just like the analysis does.

  product_summary      Q1: dashboard card for one product           PK (parent_asin)
  reviews_by_product   Q2: latest reviews of a product, newest first PK ((parent_asin), ts DESC, user_id)
  brand_monthly        Q3: monthly trend of a brand                  PK ((brand), month DESC)
"""
import os
import time

import pandas as pd
from pyspark.sql import functions as F

from . import config as C

HOST = os.environ.get("RL_CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "reviewlens"

DDL = [
    f"""CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}""",
    f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.product_summary (
        parent_asin text PRIMARY KEY, product_title text, brand text, n_reviews int,
        avg_rating double, pct_negative double, pct_verified double, last_review timestamp)""",
    f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.reviews_by_product (
        parent_asin text, ts timestamp, user_id text, rating double, review_title text,
        text text, helpful_vote int, verified_purchase boolean,
        PRIMARY KEY ((parent_asin), ts, user_id)) WITH CLUSTERING ORDER BY (ts DESC, user_id ASC)""",
    f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.brand_monthly (
        brand text, month text, reviews int, avg_rating double, pct_negative double,
        PRIMARY KEY ((brand), month)) WITH CLUSTERING ORDER BY (month DESC)""",
]


WRITERS = int(os.environ.get("RL_CASSANDRA_WRITERS", "4"))   # parallel Spark tasks writing to one small node


def connect(keyspace=None, attempts=5):
    from cassandra.cluster import Cluster, NoHostAvailable
    from cassandra.io.asyncioreactor import AsyncioConnection   # works on Python 3.12+ without libev
    for i in range(attempts):
        cluster = Cluster([HOST], connection_class=AsyncioConnection, connect_timeout=30,
                          control_connection_timeout=30)
        try:
            session = cluster.connect(keyspace)
            session.default_timeout = 60
            return cluster, session
        except NoHostAvailable:
            cluster.shutdown()
            if i == attempts - 1:
                raise
            time.sleep(3 * (i + 1))    # node busy (GC / compaction): back off and retry


def writer(insert_cql, columns):
    """Returns a function that runs on each Spark executor partition and bulk-inserts its rows."""
    def write_partition(rows):
        from cassandra.concurrent import execute_concurrent_with_args
        cluster, session = connect(KEYSPACE)
        stmt = session.prepare(insert_cql)
        buf = []
        for r in rows:
            buf.append(tuple(r[c] for c in columns))
            if len(buf) >= 2000:
                execute_concurrent_with_args(session, stmt, buf, concurrency=32)
                buf = []
        if buf:
            execute_concurrent_with_args(session, stmt, buf, concurrency=32)
        cluster.shutdown()
    return write_partition


def load(df, table, columns):
    cql = f"INSERT INTO {KEYSPACE}.{table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})"
    t0 = time.time()
    n = df.count()
    df.select(*columns).coalesce(WRITERS).foreachPartition(writer(cql, columns))
    secs = time.time() - t0
    print(f"  {table:<20} {n:>9,} rows in {secs:5.1f}s  ({n / secs:,.0f} rows/s)")
    return {"table": table, "rows": n, "seconds": round(secs, 1), "rows_per_sec": round(n / secs)}


def demo_queries(session):
    """CRUD + the three application queries, with latency, to show single-partition reads are fast."""
    out, timings = {}, []

    def timed(name, cql, params=()):
        t0 = time.time()
        rows = list(session.execute(cql, params))
        timings.append({"query": name, "cql": " ".join(cql.split()), "rows": len(rows),
                        "ms": round(1000 * (time.time() - t0), 2)})
        return rows

    top = timed("Top product by reviews (needs full scan - anti-pattern, run once)",
                f"SELECT parent_asin, n_reviews FROM {KEYSPACE}.product_summary")
    asin = max(top, key=lambda r: r.n_reviews).parent_asin
    brand = session.execute(f"SELECT brand FROM {KEYSPACE}.product_summary WHERE parent_asin=%s", (asin,)).one().brand

    out["Q1 product card"] = timed("Q1 product card", f"SELECT * FROM {KEYSPACE}.product_summary WHERE parent_asin=%s", (asin,))
    out["Q2 latest reviews"] = timed("Q2 latest 5 reviews of product",
                                     f"SELECT ts, rating, review_title FROM {KEYSPACE}.reviews_by_product "
                                     f"WHERE parent_asin=%s LIMIT 5", (asin,))
    out["Q3 brand trend"] = timed("Q3 brand monthly trend (last 12 months)",
                                  f"SELECT month, reviews, avg_rating FROM {KEYSPACE}.brand_monthly "
                                  f"WHERE brand=%s LIMIT 12", (brand,))
    out["Q4 low ratings"] = timed("Q4 product's 1-star reviews (ALLOW FILTERING inside one partition)",
                                  f"SELECT ts, review_title FROM {KEYSPACE}.reviews_by_product "
                                  f"WHERE parent_asin=%s AND rating=1.0 ALLOW FILTERING", (asin,))
    # CRUD on a test row
    timed("CREATE test product", f"INSERT INTO {KEYSPACE}.product_summary (parent_asin, product_title, brand, n_reviews,"
          f" avg_rating) VALUES ('TEST0001', 'Demo product', 'DemoBrand', 1, 5.0)")
    timed("UPDATE test product", f"UPDATE {KEYSPACE}.product_summary SET n_reviews=2, avg_rating=4.5 "
          f"WHERE parent_asin='TEST0001'")
    timed("READ test product", f"SELECT * FROM {KEYSPACE}.product_summary WHERE parent_asin='TEST0001'")
    timed("DELETE test product", f"DELETE FROM {KEYSPACE}.product_summary WHERE parent_asin='TEST0001'")

    sample = []
    for name, rows in out.items():
        for r in rows[:5]:
            sample.append({"query": name, "row": str(r._asdict())[:200]})
    return pd.DataFrame(timings), pd.DataFrame(sample), asin, brand


def run(spark):
    try:
        cluster, session = connect()
    except Exception as e:  # noqa: BLE001
        print(f"Cassandra not reachable at {HOST} ({e.__class__.__name__}). "
              "Start it with scripts/start_cassandra.sh and re-run this step.")
        C.update_summary("nosql", {"status": "skipped - Cassandra not running"})
        return
    for stmt in DDL:
        session.execute(stmt)
    for t in ("product_summary", "reviews_by_product", "brand_monthly"):
        session.execute(f"TRUNCATE {KEYSPACE}.{t}")

    df = C.load_reviews(spark)
    summary = (df.groupBy("parent_asin")
                 .agg(F.first("product_title").alias("product_title"), F.first("brand").alias("brand"),
                      F.count("*").cast("int").alias("n_reviews"), F.round(F.avg("rating"), 3).alias("avg_rating"),
                      F.round(F.avg((F.col("rating") <= 2).cast("int")), 3).alias("pct_negative"),
                      F.round(F.avg(F.col("verified_purchase").cast("int")), 3).alias("pct_verified"),
                      F.max("ts").alias("last_review")))
    reviews = (df.select("parent_asin", "ts", "user_id", "rating", "review_title",
                         F.substring("text", 1, 1000).alias("text"),
                         F.col("helpful_vote").cast("int").alias("helpful_vote"), "verified_purchase"))
    brand_monthly = (df.filter("brand IS NOT NULL").groupBy("brand", "month")
                       .agg(F.count("*").cast("int").alias("reviews"), F.round(F.avg("rating"), 3).alias("avg_rating"),
                            F.round(F.avg((F.col("rating") <= 2).cast("int")), 3).alias("pct_negative")))

    print(f"Loading Cassandra tables from Spark ({WRITERS} parallel writer partitions):")
    try:
        loads = [load(summary, "product_summary", summary.columns),
                 load(reviews, "reviews_by_product", reviews.columns),
                 load(brand_monthly, "brand_monthly", brand_monthly.columns)]
    except Exception as e:  # noqa: BLE001 - a struggling NoSQL node must not abort the whole pipeline
        print(f"Cassandra load failed: {str(e).splitlines()[0][:200]}")
        C.update_summary("nosql", {"status": "failed during load - see console output"})
        cluster.shutdown()
        return
    timings, sample, asin, brand = demo_queries(session)
    print(timings[["query", "rows", "ms"]].to_string(index=False))
    C.save_table(pd.DataFrame(loads), "cassandra_loads")
    C.save_table(timings, "cassandra_queries")
    C.save_table(sample, "cassandra_query_results")
    C.update_summary("nosql", {"status": "ok", "host": HOST, "keyspace": KEYSPACE, "loads": loads,
                               "demo_product": asin, "demo_brand": brand,
                               "single_partition_query_ms": float(timings.iloc[1:4]["ms"].median())})
    cluster.shutdown()
