"""Step 1 - Ingest the semi-structured JSON Lines files, clean them and store a curated Parquet layer.

raw JSONL (local disk or HDFS)  ->  validate / clean / enrich  ->  broadcast join with product metadata
->  Parquet partitioned by year (reviews) + Parquet (products)
"""
import os
import urllib.request

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, MapType, StringType,
                               StructField, StructType)

from . import config as C

HF = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw"

META_SCHEMA = StructType([
    StructField("parent_asin", StringType()),
    StructField("title", StringType()),
    StructField("store", StringType()),
    StructField("main_category", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", LongType()),
    StructField("price", DoubleType()),
    StructField("features", ArrayType(StringType())),
    StructField("categories", ArrayType(StringType())),
    StructField("details", MapType(StringType(), StringType())),   # free-form key/value pairs
    StructField("images", ArrayType(StructType([StructField("variant", StringType())]))),
    StructField("_corrupt_record", StringType()),
])


def download_if_missing():
    """Fetch the raw files from the Hugging Face mirror when running on a fresh machine."""
    if C.RAW_URI.startswith("hdfs://"):
        return
    for sub, name in (("review_categories", C.REVIEWS_FILE), ("meta_categories", C.META_FILE)):
        out = C.RAW_DIR / name
        if out.exists() and out.stat().st_size > 0:
            continue
        C.RAW_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {name} ...")
        urllib.request.urlretrieve(f"{HF}/{sub}/{name}", out)


def run(spark):
    download_if_missing()
    sample = float(os.environ.get("RL_SAMPLE", "1.0"))

    # Reviews: let Spark infer the nested schema (images is an array of structs)
    raw = spark.read.option("mode", "PERMISSIVE").json(C.raw_path(C.REVIEWS_FILE))
    if sample < 1.0:
        raw = raw.sample(fraction=sample, seed=42)
    raw.cache()
    raw_count = raw.count()
    print(f"Raw reviews: {raw_count:,}")
    raw.printSchema()

    quality = raw.select(
        F.sum(F.col("text").isNull().cast("int")).alias("null_text"),
        F.sum((F.length(F.trim("text")) == 0).cast("int")).alias("empty_text"),
        F.sum(F.col("rating").isNull().cast("int")).alias("null_rating"),
        F.sum((F.size("images") > 0).cast("int")).alias("with_images"),
    ).first().asDict()

    reviews = (raw
        .dropna(subset=["text", "rating", "parent_asin"])
        .filter(F.length(F.trim("text")) > 0)
        .dropDuplicates(["user_id", "parent_asin", "timestamp"])
        .withColumnRenamed("title", "review_title")
        .withColumn("ts", F.to_timestamp(F.from_unixtime((F.col("timestamp") / 1000).cast("long"))))
        .withColumn("review_date", F.to_date("ts"))
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.date_format("ts", "yyyy-MM"))
        .withColumn("hour", F.hour("ts"))
        .withColumn("weekday", F.date_format("ts", "E"))
        .withColumn("text_len", F.length("text"))
        .withColumn("word_count", F.size(F.split(F.trim("text"), r"\s+")))
        .withColumn("n_images", F.size("images"))
        .withColumn("sentiment_label", F.when(F.col("rating") >= 4, "positive")
                                        .when(F.col("rating") <= 2, "negative").otherwise("neutral"))
        .drop("images", "asin"))

    meta_raw = (spark.read.schema(META_SCHEMA).option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(C.raw_path(C.META_FILE))
                .cache())     # Spark requires caching before querying _corrupt_record alone
    corrupt_meta = meta_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    products = (meta_raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        .dropDuplicates(["parent_asin"])
        .withColumnRenamed("title", "product_title")
        .withColumn("store", F.coalesce(F.trim("store"), F.lit("Unknown")))
        .withColumn("n_features", F.coalesce(F.size("features"), F.lit(0)))
        .withColumn("n_product_images", F.coalesce(F.size("images"), F.lit(0)))
        .withColumn("brand", F.coalesce(F.col("details")["Brand"], F.col("store")))
        .withColumn("n_detail_keys", F.coalesce(F.size(F.map_keys("details")), F.lit(0)))
        .drop("features", "images", "categories", "details"))

    products.write.mode("overwrite").parquet(str(C.PRODUCTS_PARQUET))
    products = C.load_products(spark)

    # Metadata is ~10x smaller than the review table -> broadcast it to avoid shuffling the big side
    joined = reviews.join(F.broadcast(products.select("parent_asin", "product_title", "store", "brand", "price")),
                          "parent_asin", "left")
    (joined.repartition("year")
           .write.mode("overwrite").partitionBy("year").parquet(str(C.REVIEWS_PARQUET)))

    df = C.load_reviews(spark)
    clean_count = df.count()
    n_products = df.select("parent_asin").distinct().count()
    n_users = df.select("user_id").distinct().count()
    years = df.agg(F.min("year"), F.max("year")).first()
    raw_bytes = sum(C.path_size(spark, C.raw_path(f)) for f in (C.REVIEWS_FILE, C.META_FILE))
    parquet_bytes = C.path_size(spark, C.REVIEWS_PARQUET) + C.path_size(spark, C.PRODUCTS_PARQUET)

    print(f"Clean reviews: {clean_count:,} (removed {raw_count - clean_count:,})")
    print(f"Products: {n_products:,}   Users: {n_users:,}   Years: {years[0]}-{years[1]}")
    print(f"Storage: raw JSON {raw_bytes / 1e6:.0f} MB -> Parquet {parquet_bytes / 1e6:.0f} MB  ({C.LAKE_URI})")
    raw.unpersist()
    meta_raw.unpersist()

    C.update_summary("ingest", {
        "category": C.CATEGORY, "raw_reviews": raw_count, "clean_reviews": clean_count,
        "removed_rows": raw_count - clean_count, "products_in_metadata": products.count(),
        "corrupt_metadata_rows": corrupt_meta, "reviewed_products": n_products, "users": n_users,
        "first_year": years[0], "last_year": years[1], "raw_mb": round(raw_bytes / 1e6, 1),
        "parquet_mb": round(parquet_bytes / 1e6, 1), "quality": quality,
    })
