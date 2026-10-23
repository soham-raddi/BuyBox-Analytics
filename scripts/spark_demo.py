"""Live Spark demo: runs a few jobs on the HDFS data lake and keeps the Spark UI (http://localhost:4040) open.

    source scripts/env.sh && python scripts/spark_demo.py      (HDFS must be running)
"""
import os
import re
import shutil
import subprocess
import sys
from operator import add
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pyspark.sql import SparkSession, functions as F  # noqa: E402

LAKE = "hdfs://localhost:9000/reviewlens/curated/reviews"


def banner(t):
    print(f"\n\033[1;36m===== {t} =====\033[0m", flush=True)


spark = (SparkSession.builder.appName("BuyBox Analytics - live demo").master("local[*]")
         .config("spark.driver.memory", "3g").config("spark.sql.shuffle.partitions", "16")
         .config("spark.ui.port", "4040").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"\033[1;33mSpark {spark.version} running. Spark UI: http://localhost:4040\033[0m")
if shutil.which("explorer.exe") and not os.environ.get("NO_BROWSER"):   # WSL: open the UI in the Windows browser
    subprocess.Popen(["explorer.exe", "http://localhost:4040"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

banner("1. Read the Parquet data lake from HDFS")
df = spark.read.parquet(LAKE)
df.printSchema()
print(f"Reviews: {df.count():,}")

banner("2. Cache the table (see the Storage tab)")
df = df.cache()
print(f"Cached {df.count():,} rows in memory")

banner("3. DataFrame API: rating distribution")
df.groupBy("rating").count().orderBy("rating").show()

banner("4. Spark SQL: most-reviewed products")
df.createOrReplaceTempView("reviews")
spark.sql("""
    SELECT SUBSTRING(product_title, 1, 45) AS product, COUNT(*) AS reviews, ROUND(AVG(rating), 2) AS avg_rating
    FROM reviews GROUP BY product_title ORDER BY reviews DESC LIMIT 5""").show(truncate=False)

banner("5. Partition pruning: only the year=2021 folder is read")
print(f"Reviews in 2021: {spark.read.parquet(LAKE).filter('year = 2021').count():,}")

banner("6. RDD MapReduce: top complaint words (flatMap -> map -> reduceByKey)")
token = re.compile(r"[a-z']{4,}")
stop = spark.sparkContext.broadcast({"this", "that", "with", "have", "they", "were", "from", "would", "product",
                                     "just", "like", "after", "these", "what", "will", "your", "there", "when",
                                     "them", "than", "then", "only", "been", "it's", "very", "really", "because"})
top = (df.filter("rating <= 2").select("text").rdd
         .flatMap(lambda r: [w for w in token.findall((r.text or "").lower()) if w not in stop.value])
         .map(lambda w: (w, 1)).reduceByKey(add).takeOrdered(10, key=lambda kv: -kv[1]))
for w, c in top:
    print(f"  {w:<14}{c:>8,}")

banner("7. Broadcast join with product metadata")
products = spark.read.parquet("hdfs://localhost:9000/reviewlens/curated/products").select("parent_asin", "average_rating")
joined = df.join(F.broadcast(products), "parent_asin")
print(f"Joined rows: {joined.count():,}  (see the SQL tab for the BroadcastHashJoin plan)")

print("\n\033[1;33mAll jobs finished. The Spark UI stays open at http://localhost:4040")
print("Tabs to show: Jobs, Stages, Storage (cached table), Environment, Executors, SQL / DataFrame\033[0m")
input("\nPress Enter to stop Spark and close the UI... ")
spark.stop()
