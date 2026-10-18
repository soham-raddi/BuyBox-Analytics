#!/usr/bin/env bash
# Simulate the MapReduce data flow with Unix pipes (map | shuffle-sort | reduce) - no Hadoop needed.
# Handy for testing the mapper/reducer logic on a small file before submitting to the cluster.
#   bash mapreduce/run_local.sh [input.jsonl] [lines]
set -eu
IN=$(realpath "${1:-$(dirname "$0")/../data/raw/All_Beauty.jsonl}")
cd "$(dirname "$0")"
N=${2:-50000}
echo "Word count on the first $N reviews:"
head -n "$N" "$IN" | python3 wordcount_mapper.py 2>/dev/null | sort | python3 wordcount_reducer.py \
  | sort -t$'\t' -k2,2nr | head -10
echo "Product stats on the first $N reviews (top 5 by count):"
head -n "$N" "$IN" | python3 product_stats_mapper.py 2>/dev/null | sort | python3 product_stats_reducer.py \
  | sort -t$'\t' -k2,2nr | head -5
