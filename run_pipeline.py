"""BuyBox Analytics pipeline runner.

    python run_pipeline.py                      # all Spark steps
    python run_pipeline.py --steps ingest,analytics
    RL_SAMPLE=0.2 python run_pipeline.py        # quick run on a 20% sample
"""
import argparse
import time

from reviewlens import analytics, config, graph_segments, ingest, nosql, performance, sentiment, streaming

STEPS = {
    "ingest": ("1. Ingest JSON -> clean -> Parquet", ingest.run),
    "analytics": ("2. Descriptive analytics (DataFrame / SQL / RDD)", analytics.run),
    "sentiment": ("3. Sentiment model (MLlib) + rating mismatch", sentiment.run),
    "graph": ("4. Co-review network + product segmentation", graph_segments.run),
    "streaming": ("5. Structured Streaming replay", streaming.run),
    "nosql": ("6. Cassandra serving layer", nosql.run),
    "performance": ("7. Performance evaluation", performance.run),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default=",".join(STEPS), help=f"comma list from: {', '.join(STEPS)}")
    args = ap.parse_args()
    chosen = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = set(chosen) - set(STEPS)
    if unknown:
        ap.error(f"unknown steps: {unknown}")

    spark = config.get_spark()
    print(f"Spark {spark.version} | master {spark.sparkContext.master} | "
          f"default parallelism {spark.sparkContext.defaultParallelism} | input {config.RAW_URI}")
    t0 = time.time()
    timings = {}
    for name in chosen:
        title, fn = STEPS[name]
        s0 = time.time()
        with config.step(title):
            fn(spark)
        timings[name] = round(time.time() - s0, 1)
    previous = config.read_summary().get("run", {}).get("step_seconds", {})
    config.update_summary("run", {"spark_version": spark.version, "master": spark.sparkContext.master,
                                  "step_seconds": {**previous, **timings},
                                  "finished": time.strftime("%Y-%m-%d %H:%M")})
    print(f"\nDone in {time.time() - t0:.0f}s. Figures: {config.FIG_DIR}  Tables: {config.TABLE_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
