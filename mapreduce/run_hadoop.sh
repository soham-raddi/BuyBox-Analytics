#!/usr/bin/env bash
# Run both Hadoop Streaming MapReduce jobs on the raw JSON stored in HDFS.
#   bash mapreduce/run_hadoop.sh            (LocalJobRunner, needs only HDFS)
#   MR_FRAMEWORK=yarn bash mapreduce/run_hadoop.sh   (submit to YARN)
set -euo pipefail
source "$(dirname "$0")/../scripts/env.sh"
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)
IN=/reviewlens/raw/All_Beauty.jsonl
OUT=/reviewlens/mapreduce
# Hadoop's -files option cannot handle spaces in paths, so ship the scripts from /tmp
MR=/tmp/reviewlens_mr
mkdir -p "$MR" && cp "$PROJECT_DIR"/mapreduce/*.py "$MR"/
TABLES="$PROJECT_DIR/outputs/tables"
PY="$(command -v python3)"
mkdir -p "$TABLES"

run_job () {   # name mapper reducer combiner reducers
  local name=$1
  hdfs dfs -rm -r -f -skipTrash "$OUT/$name" > /dev/null 2>&1
  local t0=$(date +%s)
  local combiner=()
  [ -n "$4" ] && combiner=(-combiner "$PY $4")
  hadoop jar "$STREAMING_JAR" \
    -D mapreduce.framework.name="${MR_FRAMEWORK:-local}" \
    -D mapreduce.job.name="BuyBoxAnalytics-$name" \
    -D mapreduce.job.reduces="$5" \
    -files "$MR/$2,$MR/$3" \
    -input "$IN" -output "$OUT/$name" \
    -mapper "$PY $2" "${combiner[@]}" -reducer "$PY $3" 2>"$PROJECT_DIR/outputs/mr_$name.log" \
    || { echo "Job $name failed - see outputs/mr_$name.log"; exit 1; }
  echo "Job $name finished in $(( $(date +%s) - t0 ))s"
  sed -n '/completed successfully/,$p' "$PROJECT_DIR/outputs/mr_$name.log" | grep -E "Launched map tasks|Launched reduce tasks|Map input records|Map output records|Combine output records|Reduce input groups|Reduce output records|MalformedJSON|NegativeReviews|BadRecords|HDFS: Number of bytes read=" | sed 's/^\s*/    /' | head -20
}

echo "== Job 1: complaint word count (rating <= 2) =="
run_job complaint_wordcount wordcount_mapper.py wordcount_reducer.py wordcount_reducer.py 2
hdfs dfs -cat "$OUT/complaint_wordcount/part-*" | sort -t$'\t' -k2,2nr > "$TABLES/mr_complaint_wordcount.tsv"
echo "Top 5 complaint words:"; head -5 "$TABLES/mr_complaint_wordcount.tsv" | sed 's/^/    /'

echo "== Job 2: per-product rating statistics =="
run_job product_stats product_stats_mapper.py product_stats_reducer.py "" 4
{ printf "parent_asin\tn_reviews\tavg_rating\tpct_negative\thelpful_votes\tpct_verified\tr1\tr2\tr3\tr4\tr5\n"
  hdfs dfs -cat "$OUT/product_stats/part-*" | sort -t$'\t' -k2,2nr; } > "$TABLES/mr_product_stats.tsv"
echo "Most-reviewed products:"; head -6 "$TABLES/mr_product_stats.tsv" | column -t | sed 's/^/    /'
hdfs dfs -ls -h "$OUT"/*/ | grep part- | sed 's/^/    /'
