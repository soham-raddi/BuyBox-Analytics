# Live demo: run BuyBox Analytics from scratch

Use this when you have to execute the project in front of the teacher. Every result is produced live:
HDFS is formatted, the data is uploaded, MapReduce and Spark run, Cassandra is loaded, then the dashboard opens.

**Total time:** about 17 minutes (measured in a full rehearsal), or about 10 with the quick option in step 6.

| Step | Time (measured) |
|---|---|
| 1. Environment (first `hadoop` command after WSL starts) | ~45 s |
| 2. Format and start HDFS | ~40 s |
| 3. Upload 540 MB to HDFS | ~70 s |
| 4. Two MapReduce jobs | ~1.5 min |
| 5. Start Cassandra | ~20 s |
| 6. Spark: ingest + analytics / sentiment + graph / streaming / Cassandra load / benchmarks | 1.3 / 3 / 2 / 2.5 / 1.7 min |

> **Automated version:** double-click **`BuyBox_Analytics.bat`** (or the *BuyBox Analytics* shortcut on the
> Desktop) and choose **1. Live demo from scratch**. It runs exactly the steps below, showing each command and
> pausing between steps, and opens the HDFS UI, Spark UI and dashboard automatically. Use the manual steps below
> if the teacher wants to see you type the commands.

---

## Before the demo (5 minutes before)

Open **Ubuntu** from the Start menu (or type `wsl -d Ubuntu-24.04` in PowerShell) and reset everything:

```bash
cd "/mnt/c/Users/Soham/Codes/random stuff/bda mini project"
bash scripts/reset_demo.sh
```

This stops all services and deletes HDFS storage, Cassandra data and all generated results. It keeps the dataset
in `data/raw`. Close any open dashboard window.

> Keep this **one Ubuntu window open for the whole demo**. HDFS runs inside it; closing it stops HDFS.

---

## Step 1: Environment

```bash
cd "/mnt/c/Users/Soham/Codes/random stuff/bda mini project"
source scripts/env.sh
java -version
hadoop version | head -1
python -c "import pyspark; print('Spark', pyspark.__version__)"
```

*Say:* "Everything runs on a single-node, pseudo-distributed cluster in WSL Ubuntu: Java 17, Hadoop 3.3, Spark 3.5,
Cassandra 5.0."

## Step 2: Format and start HDFS

```bash
HADOOP_ROOT_LOGGER=INFO,console hdfs namenode -format -nonInteractive 2>&1 | grep -i "formatted"
hdfs --daemon start namenode
hdfs --daemon start datanode
jps
bash scripts/wait_for_hdfs.sh
```

`jps` should list **NameNode** and **DataNode**. The NameNode needs about 10–20 seconds to start accepting
connections; `wait_for_hdfs.sh` waits for that, then for "Safe mode is OFF". Open **http://localhost:9870** in the browser and show
*Overview* and *Datanodes*.

*Say:* "Formatting creates an empty file system namespace. The NameNode stores metadata, the DataNode stores the
blocks."

## Step 3: Load the dataset into HDFS

```bash
ls -lh data/raw
hdfs dfs -mkdir -p /reviewlens/raw
hdfs dfs -put data/raw/*.jsonl /reviewlens/raw/
hdfs dfs -ls -h /reviewlens/raw
hdfs dfs -stat "replication=%r  blocksize=%o  size=%b" /reviewlens/raw/All_Beauty.jsonl
hdfs fsck /reviewlens/raw/All_Beauty.jsonl -files -blocks | grep -E "blk_|Status"
hdfs dfs -cat /reviewlens/raw/All_Beauty.jsonl | head -1
```

*Say:* "The 327 MB review file is split into **5 blocks of 64 MB**. The last command reads one semi-structured JSON
review straight from HDFS." In the web UI, open *Utilities → Browse the file system → /reviewlens/raw* and click
the file to show its blocks.

## Step 4: MapReduce on HDFS (about 1 minute)

```bash
cat mapreduce/wordcount_mapper.py | tail -12
bash mapreduce/run_hadoop.sh
hdfs dfs -ls /reviewlens/mapreduce/complaint_wordcount
```

*Say:* "Hadoop Streaming runs our Python mapper and reducer. The mapper parses each JSON line and emits (word, 1)
for 1–2★ reviews, the combiner sums locally, and the reducer sums per word. The counters show map input records,
combine output (95% less shuffle) and our custom NegativeReviews counter."

## Step 5: Start Cassandra (about 30 seconds)

```bash
bash scripts/start_cassandra.sh
cqlsh -e "DESCRIBE KEYSPACES;"
```

## Step 6: Spark pipeline, reading from HDFS

```bash
export RL_RAW_URI=hdfs://localhost:9000/reviewlens/raw
```

**Short on time?** Run `export RL_SAMPLE=0.2` now. Spark then uses a 20% sample, and step 6 takes about
4 minutes instead of about 10. The numbers will differ slightly from the report.

While a step is running, open **http://localhost:4040** to show the **Spark UI** (Jobs, Stages, SQL). It exists only
while a step runs.

```bash
python run_pipeline.py --steps ingest,analytics
```

About 1 minute. Spark reads the JSON from HDFS, cleans it, does a broadcast join, writes Parquet partitioned by year
back to HDFS, then runs SQL, DataFrame and RDD analytics.

```bash
hdfs dfs -ls /reviewlens/curated/reviews | head
hdfs dfs -du -h /reviewlens
```

*Say:* "540 MB of JSON became 184 MB of Parquet, partitioned by year."

```bash
python run_pipeline.py --steps sentiment,graph
```

About 3 minutes: the MLlib sentiment model (watch the accuracy table print), the co-review network with PageRank,
and K-Means segments.

```bash
python run_pipeline.py --steps streaming
```

About 1.5 minutes: Structured Streaming replays reviews and prints alerts as micro-batches arrive.

```bash
python run_pipeline.py --steps nosql
cqlsh -e "SELECT parent_asin, brand, n_reviews, avg_rating FROM reviewlens.product_summary LIMIT 5;"
```

About 2–3 minutes: Spark loads about 1 million rows into Cassandra, then runs the CRUD and latency queries.

```bash
python run_pipeline.py --steps performance
```

Optional, about 1.5 minutes: the optimisation benchmarks (Parquet vs JSON, caching, partitions).

## Step 7: Dashboard

In **Windows**, double-click **`BuyBox_Analytics.bat`** and choose **5. Open the dashboard**. It opens
http://localhost:8501 and shows the results that were just generated.

## Step 8: Stop everything

```bash
bash scripts/stop_services.sh
```

---

## If something goes wrong

| Problem | Fix |
|---|---|
| `hdfs: command not found` | You skipped `source scripts/env.sh` (step 1); run it again. |
| `jps` shows only DataNode (no NameNode) | The `hdfs --daemon start namenode` line was skipped. Run it, then `bash scripts/wait_for_hdfs.sh`. Don't format again. |
| `Connection refused ... 9000` | The NameNode is still starting (or not running). Run `bash scripts/wait_for_hdfs.sh`; if `jps` shows no NameNode, rerun step 2's `hdfs --daemon start ...` lines. |
| Safe-mode message | Run `bash scripts/wait_for_hdfs.sh` and try again. |
| First `hadoop` command takes ~45 s | Normal right after Windows starts WSL: Java loads Hadoop's jars for the first time. Run step 1 before the teacher arrives to warm it up. |
| Cassandra step says "not reachable" | Run `bash scripts/start_cassandra.sh` again, wait for "ready", then rerun `--steps nosql`. |
| A Spark step fails mid-demo | Rerun only that step. Steps can be rerun in any order once `ingest` has run. |
| Need the results back quickly | `git checkout -- outputs` restores the committed results; the dashboard works with them. |
| Dashboard shows old numbers | Close it and open it again (option 5 in `BuyBox_Analytics.bat`). |

## One-command alternative

If the teacher just wants to see it run end to end without explanation: after the reset, run `bash run_all.sh`
(about 11 minutes). It does steps 2–6 automatically.
