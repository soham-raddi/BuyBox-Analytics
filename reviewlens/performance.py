"""Step 7 - Performance evaluation of the optimisation techniques used in the pipeline.

Each experiment runs the same query two (or more) ways and reports the best of N wall-clock runs.
"""
import re
import time

import pandas as pd
from pyspark.sql import functions as F

from . import config as C
from .viz import BLUE, GREEN, ORANGE, PURPLE, RED, plt


def timed(fn, repeats=3):
    best = float("inf")
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return round(best, 3)


def plot(perf, parts, scale):
    """Draw figure 12 (also callable on the saved CSVs: python -m reviewlens.performance)."""
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), gridspec_kw={"width_ratios": [1.7, 1, 1]})
    labels = [f"{e}: {s}" for e, s in zip(perf["experiment"], perf["setup"])]
    colors = [(RED, GREEN)[i % 2] for i in range(len(perf))]
    axes[0].barh(labels[::-1], perf["seconds"][::-1], color=colors[::-1])
    for y, v in enumerate(perf["seconds"][::-1]):
        axes[0].text(v, y, f" {v:.2f}s", va="center", fontsize=8)
    axes[0].set(xlabel="Seconds (lower is better)", title="Baseline (red) vs optimised (green)")
    axes[1].plot(parts["partitions"], parts["seconds"], marker="o", color=PURPLE)
    axes[1].set_xscale("log", base=2)
    axes[1].set(xlabel="Partitions (parallel tasks)", ylabel="Seconds", title="Parallelism (RDD word count)")
    axes[2].plot(scale["rows"] / 1e3, scale["seconds"], marker="s", color=BLUE)
    axes[2].set(xlabel="Rows (thousands)", ylabel="Seconds", title="Scalability (RDD word count)")
    C.save_fig(fig, "12_performance")


def run(spark):
    rows = []

    def add(group, setup, secs):
        rows.append({"experiment": group, "setup": setup, "seconds": secs})
        print(f"  {group:<22} {setup:<38} {secs:7.2f}s")

    json_path = C.raw_path(C.REVIEWS_FILE)
    pq = str(C.REVIEWS_PARQUET)
    last_year = spark.read.parquet(pq).agg(F.max("year")).first()[0]

    # 1. Storage format (row-oriented text vs columnar binary)
    add("Storage format", "JSON Lines (raw)", timed(lambda: spark.read.json(json_path).filter("rating = 1").count(), 2))
    add("Storage format", "Parquet (columnar, compressed)", timed(lambda: spark.read.parquet(pq).filter("rating = 1").count()))

    # 2. Partition pruning: year filter only reads one directory of the partitioned table
    add("Partition pruning", "JSON - filter by year (full scan)",
        timed(lambda: spark.read.json(json_path)
              .filter(F.year(F.from_unixtime(F.col("timestamp") / 1000)) == last_year).count(), 2))
    add("Partition pruning", "Parquet - partitionBy(year)",
        timed(lambda: spark.read.parquet(pq).filter(F.col("year") == last_year).count()))

    # 3. Column pruning: Parquet only reads the columns a query needs
    add("Column pruning", "Parquet - avg(length(text))", timed(lambda: spark.read.parquet(pq).agg(F.avg(F.length("text"))).first()))
    add("Column pruning", "Parquet - avg(rating)", timed(lambda: spark.read.parquet(pq).agg(F.avg("rating")).first()))

    # 4. Caching: an iterative workload (several queries on the same cleaned data). Without cache every
    #    action recomputes the lineage from the raw JSON: parse -> clean -> filter.
    def cleaned():
        return (spark.read.json(json_path).dropna(subset=["text", "rating"])
                     .withColumn("year", F.year(F.from_unixtime(F.col("timestamp") / 1000))))

    def three_queries(d):
        d.groupBy("rating").count().collect()
        d.groupBy("year").agg(F.avg("rating")).collect()
        d.groupBy("parent_asin").count().count()

    add("Caching", "No cache (lineage recomputed from JSON)", timed(lambda: three_queries(cleaned()), 2))
    cached_clean = cleaned().cache()
    cached_clean.count()
    add("Caching", "cache() after cleaning", timed(lambda: three_queries(cached_clean)))
    cached_clean.unpersist()

    # 5. Join strategy: broadcast hash join vs sort-merge join with the product table (AQE off so the
    #    planner cannot silently switch strategies at runtime)
    cached = spark.read.parquet(pq).cache()
    cached.count()
    products = C.load_products(spark).select("parent_asin", F.col("store").alias("p_store"), "average_rating")
    join_q = lambda p: (cached.join(p, "parent_asin").groupBy("p_store")   # noqa: E731
                              .agg(F.avg("rating"), F.avg("average_rating")).count())
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    add("Join strategy", "Sort-merge join (shuffle both sides)", timed(lambda: join_q(products)))
    add("Join strategy", "Broadcast hash join", timed(lambda: join_q(F.broadcast(products))))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

    # 6/7. A CPU-heavy job (tokenise every review + word count, MapReduce style on RDDs)
    token = re.compile(r"[a-z']{3,}")

    def word_count(d, partitions):
        return (d.select("text").rdd.repartition(partitions)
                 .flatMap(lambda r: token.findall((r.text or "").lower()))
                 .map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b, numPartitions=partitions).count())

    cores = spark.sparkContext.defaultParallelism
    part_rows = []
    for n in [1, 2, 4, 8, 16, 32]:
        secs = timed(lambda: word_count(cached, n), 1)
        part_rows.append({"partitions": n, "seconds": secs})
        print(f"  {'Parallelism':<22} {n:>3} partitions {'':<24} {secs:7.2f}s")

    scale_rows = []
    for frac in [0.125, 0.25, 0.5, 1.0]:
        d = cached.sample(fraction=frac, seed=1) if frac < 1 else cached
        n = d.count()
        secs = timed(lambda: word_count(d, cores), 1)
        scale_rows.append({"fraction": frac, "rows": n, "seconds": secs})
        print(f"  {'Scalability':<22} {n:>9,} rows {'':<23} {secs:7.2f}s")
    cached.unpersist()

    perf = pd.DataFrame(rows)
    parts = pd.DataFrame(part_rows)
    scale = pd.DataFrame(scale_rows)
    C.save_table(perf, "performance_experiments")
    C.save_table(parts, "performance_partitions")
    C.save_table(scale, "performance_scalability")
    plot(perf, parts, scale)

    def speedup(exp):
        s = perf[perf["experiment"] == exp]["seconds"].tolist()
        return round(s[0] / max(s[1], 1e-3), 1)

    C.update_summary("performance", {
        "parquet_vs_json_speedup": speedup("Storage format"),
        "partition_pruning_speedup": speedup("Partition pruning"),
        "column_pruning_speedup": speedup("Column pruning"),
        "cache_speedup": speedup("Caching"),
        "broadcast_join_speedup": speedup("Join strategy"),
        "best_partitions": int(parts.loc[parts["seconds"].idxmin(), "partitions"]),
        "single_partition_seconds": float(parts.iloc[0]["seconds"]),
        "best_partition_seconds": float(parts["seconds"].min()),
    })


if __name__ == "__main__":
    plot(pd.read_csv(C.TABLE_DIR / "performance_experiments.csv"), pd.read_csv(C.TABLE_DIR / "performance_partitions.csv"),
         pd.read_csv(C.TABLE_DIR / "performance_scalability.csv"))
