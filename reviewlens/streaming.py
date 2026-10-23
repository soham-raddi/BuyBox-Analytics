"""Step 5 - Real-time analytics with Spark Structured Streaming.

A producer thread replays the most recent months of reviews in time order, writing small JSON files
into a landing folder (the role Kafka / Flume would play in production). A streaming query reads the
folder, uses event-time windows + watermarks, and every micro-batch updates:
  * a live per-day KPI table (volume, avg rating, negative share)
  * product alerts: a product gets >= 5 negative reviews in a 7-day window and they are >= 40% of its reviews
"""
import json
import os
import shutil
import threading
import time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType, StructField, StructType,
                               TimestampType)

from . import config as C
from .viz import BLUE, ORANGE, RED, plt

SCHEMA = StructType([
    StructField("parent_asin", StringType()), StructField("product_title", StringType()),
    StructField("rating", DoubleType()), StructField("ts", TimestampType()),
    StructField("verified_purchase", BooleanType()), StructField("helpful_vote", LongType()),
])

IN_DIR = C.STREAM_DIR / "incoming"
CKPT_DIR = C.STREAM_DIR / "checkpoint"


def producer(rows, batch_size, delay, done):
    """Replay events in time order as small JSON-lines files."""
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        tmp = IN_DIR / f".batch_{i:08d}.tmp"
        with open(tmp, "w") as f:
            for r in chunk:
                f.write(json.dumps(r, default=str) + "\n")
        os.replace(tmp, IN_DIR / f"batch_{i:08d}.json")   # atomic: the stream never sees half a file
        time.sleep(delay)
    done.set()


def run(spark):
    # Replay the busiest period of the dataset (early 2021) by default
    start = pd.Timestamp(os.environ.get("RL_STREAM_START", "2021-01-01")).to_pydatetime()
    months = int(os.environ.get("RL_STREAM_MONTHS", "3"))
    end = (pd.Timestamp(start) + pd.DateOffset(months=months)).to_pydatetime()
    batch_size = int(os.environ.get("RL_STREAM_BATCH", "1000"))
    # Alert rule: >= 5 negatives in 7 days on the full data; scaled down when running on a sample (quick mode)
    min_negatives = max(2, round(5 * float(os.environ.get("RL_SAMPLE", "1.0"))))
    for d in (IN_DIR, CKPT_DIR):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)

    df = C.load_reviews(spark)
    events = (df.filter((F.col("ts") >= F.lit(start)) & (F.col("ts") < F.lit(end)))
                .select("parent_asin", F.substring("product_title", 1, 60).alias("product_title"),
                        "rating", F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("ts"),
                        "verified_purchase", "helpful_vote")
                .orderBy("ts").toPandas().to_dict("records"))
    print(f"Replaying {len(events):,} reviews from {start:%Y-%m-%d} to {end:%Y-%m-%d} "
          f"in files of {batch_size} events")

    stream = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 2).json(C.local_uri(IN_DIR))
                   .withWatermark("ts", "10 days"))

    daily = (stream.groupBy(F.window("ts", "1 day").alias("w"))
                   .agg(F.count("*").alias("reviews"), F.avg("rating").alias("avg_rating"),
                        F.avg((F.col("rating") <= 2).cast("int")).alias("negative_share")))
    product_window = (stream.groupBy(F.window("ts", "7 days", "1 day").alias("w"), "parent_asin")
                            .agg(F.first("product_title").alias("product_title"), F.count("*").alias("reviews"),
                                 F.sum((F.col("rating") <= 2).cast("int")).alias("negatives"),
                                 F.avg("rating").alias("avg_rating")))

    state = {"daily": {}, "alerts": {}}
    batch_log = []

    def on_daily(batch, batch_id):
        t0 = time.time()
        for r in batch.collect():
            state["daily"][r.w.start] = (r.reviews, r.avg_rating, r.negative_share)
        batch_log.append({"batch_id": batch_id, "updated_windows": len(state["daily"]),
                          "handler_ms": round(1000 * (time.time() - t0), 1)})

    def on_products(batch, batch_id):
        hits = batch.filter(f"negatives >= {min_negatives} AND negatives / reviews >= 0.4").collect()
        for r in hits:
            key = (r.parent_asin, r.w.start)
            state["alerts"][key] = {"window_start": r.w.start, "window_end": r.w.end, "parent_asin": r.parent_asin,
                                    "product": r.product_title, "reviews": r.reviews, "negatives": r.negatives,
                                    "avg_rating": round(r.avg_rating, 2), "detected_in_batch": batch_id}

    q1 = (daily.writeStream.outputMode("update").foreachBatch(on_daily)
               .option("checkpointLocation", C.local_uri(CKPT_DIR / "daily")).trigger(processingTime="2 seconds").start())
    q2 = (product_window.writeStream.outputMode("update").foreachBatch(on_products)
                        .option("checkpointLocation", C.local_uri(CKPT_DIR / "products"))
                        .trigger(processingTime="2 seconds").start())

    done = threading.Event()
    t0 = time.time()
    threading.Thread(target=producer, args=(events, batch_size, 0.5, done), daemon=True).start()
    n_files = (len(events) + batch_size - 1) // batch_size
    while True:
        time.sleep(2)
        processed = sum(p["numInputRows"] for p in q1.recentProgress) if q1.recentProgress else 0
        print(f"  t={time.time() - t0:5.1f}s  files produced={len(list(IN_DIR.glob('batch_*.json')))}/{n_files}"
              f"  rows processed (recent)={processed:,}  alerts={len(state['alerts'])}", flush=True)
        if done.is_set() and not q1.status["isDataAvailable"] and not q2.status["isDataAvailable"]:
            time.sleep(4)   # let the last trigger finish
            if not q1.status["isDataAvailable"] and not q2.status["isDataAvailable"]:
                break
    elapsed = time.time() - t0
    progress = [{"batch_id": p["batchId"], "input_rows": p["numInputRows"],
                 "rows_per_sec": round(p.get("processedRowsPerSecond") or 0, 1),
                 "duration_ms": p["durationMs"].get("triggerExecution", 0)} for p in q1.recentProgress]
    q1.stop(); q2.stop()

    daily_df = (pd.DataFrame([(k, *v) for k, v in state["daily"].items()],
                             columns=["day", "reviews", "avg_rating", "negative_share"]).sort_values("day"))
    alerts = pd.DataFrame(list(state["alerts"].values()))
    if len(alerts):
        alerts = (alerts.sort_values("negatives", ascending=False)
                        .drop_duplicates("parent_asin").sort_values("window_start"))
    C.save_table(daily_df, "stream_daily_kpis")
    C.save_table(alerts, "stream_alerts")
    C.save_table(pd.DataFrame(progress), "stream_batches")

    fig, axes = plt.subplots(3, 1, figsize=(11, 8.5), sharex=True,
                             gridspec_kw={"height_ratios": [1.2, 1, 0.8]})
    axes[0].bar(daily_df["day"], daily_df["reviews"], color=BLUE, width=0.9)
    axes[0].set(ylabel="Reviews / day", title="Streaming output: event-time daily windows")
    axes[1].plot(daily_df["day"], daily_df["avg_rating"].rolling(7, min_periods=1).mean(), color=ORANGE, lw=2,
                 label="Avg rating (7-day rolling)")
    ax_neg = axes[1].twinx()
    ax_neg.plot(daily_df["day"], 100 * daily_df["negative_share"].rolling(7, min_periods=1).mean(), color=RED,
                lw=1, alpha=0.7, label="% negative")
    ax_neg.set_ylabel("% negative", color=RED)
    ax_neg.grid(False)
    axes[1].set_ylabel("Avg rating", color=ORANGE)
    if len(alerts):
        per_day = alerts.groupby(pd.to_datetime(alerts["window_start"]).dt.floor("D")).size()
        axes[2].stem(per_day.index, per_day.values, linefmt=RED, markerfmt="o", basefmt=" ")
    axes[2].set(ylabel="New alerts", title="Product negative-spike alerts raised by the stream")
    C.save_fig(fig, "11_streaming_dashboard")

    total_rows = sum(p["input_rows"] for p in progress)
    print(f"Stream finished: {len(events):,} events in {elapsed:.1f}s, {len(progress)} micro-batches, "
          f"{len(alerts)} products flagged")
    C.update_summary("streaming", {
        "replayed_events": len(events), "window_start": str(start)[:10], "window_end": str(end)[:10],
        "files": n_files, "micro_batches": len(progress), "rows_seen_in_progress": total_rows,
        "wall_seconds": round(elapsed, 1),
        "avg_rows_per_sec": round(sum(p["rows_per_sec"] for p in progress) / max(len(progress), 1), 1),
        "products_flagged": int(len(alerts)),
        "top_alert": alerts.iloc[0]["product"] if len(alerts) else None,
    })
