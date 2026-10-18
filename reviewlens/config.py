"""Shared paths, Spark session factory and small output helpers used by every pipeline step."""
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def local_uri(path):
    """file:// URI without %-escaping (Hadoop's Path class does not decode %20 in folder names with spaces)."""
    return "file://" + Path(path).resolve().as_posix()


CATEGORY = os.environ.get("RL_CATEGORY", "All_Beauty")

# Raw input can live on the local disk or in HDFS (set RL_RAW_URI=hdfs://localhost:9000/reviewlens/raw)
RAW_DIR = ROOT / "data" / "raw"
RAW_URI = os.environ.get("RL_RAW_URI", local_uri(RAW_DIR))
REVIEWS_FILE = f"{CATEGORY}.jsonl"
META_FILE = f"meta_{CATEGORY}.jsonl"

# Curated Parquet "data lake" + models: kept in HDFS when the raw input comes from HDFS, else on local disk
LAKE_URI = os.environ.get("RL_LAKE_URI") or (
    RAW_URI.rsplit("/", 1)[0] if RAW_URI.startswith("hdfs://") else local_uri(ROOT / "data"))
REVIEWS_PARQUET = f"{LAKE_URI}/curated/reviews"      # partitioned by year
PRODUCTS_PARQUET = f"{LAKE_URI}/curated/products"
MODEL_URI = f"{LAKE_URI}/models"
STREAM_DIR = ROOT / "data" / "stream"              # streaming landing zone is always local

OUT_DIR = ROOT / "outputs"
FIG_DIR = OUT_DIR / "figures"
TABLE_DIR = OUT_DIR / "tables"
SUMMARY_FILE = OUT_DIR / "summary.json"

for d in (STREAM_DIR, FIG_DIR, TABLE_DIR):
    d.mkdir(parents=True, exist_ok=True)


def raw_path(name):
    return f"{RAW_URI.rstrip('/')}/{name}"


def path_size(spark, uri):
    """Bytes under a local or HDFS path, using Hadoop's FileSystem API through the Spark JVM."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.getContentSummary(path).getLength() if fs.exists(path) else 0


def get_spark(app="BuyBoxAnalytics"):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName(app)
             .master(os.environ.get("RL_MASTER", "local[*]"))
             .config("spark.driver.memory", os.environ.get("RL_DRIVER_MEMORY", "4g"))
             .config("spark.sql.shuffle.partitions", "32")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_reviews(spark):
    return spark.read.parquet(REVIEWS_PARQUET)


def load_products(spark):
    return spark.read.parquet(PRODUCTS_PARQUET)


def save_table(pdf, name):
    path = TABLE_DIR / f"{name}.csv"
    pdf.to_csv(path, index=False)
    return path


def save_fig(fig, name):
    import matplotlib.pyplot as plt
    fig.tight_layout()
    fig.savefig(FIG_DIR / f"{name}.png", dpi=150)
    plt.close(fig)


def read_summary():
    return json.loads(SUMMARY_FILE.read_text()) if SUMMARY_FILE.exists() else {}


def update_summary(section, values):
    """Merge one step's headline numbers into outputs/summary.json (read by the dashboard and report)."""
    data = read_summary()
    data[section] = values
    SUMMARY_FILE.write_text(json.dumps(data, indent=2, default=str))


@contextmanager
def step(title):
    print(f"\n=== {title} ===", flush=True)
    t0 = time.time()
    yield
    print(f"--- {title} finished in {time.time() - t0:.1f}s", flush=True)
