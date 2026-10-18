#!/usr/bin/env bash
# Run selected Spark pipeline steps, e.g.  bash scripts/run_step.sh analytics,sentiment
# Reads the raw data from HDFS when the NameNode is up, otherwise from data/raw on the local disk.
source "$(dirname "$0")/env.sh"
if hdfs dfs -test -e /reviewlens/raw/All_Beauty.jsonl 2>/dev/null; then
  export RL_RAW_URI=hdfs://localhost:9000/reviewlens/raw
fi
python run_pipeline.py --steps "${1:-ingest,analytics,sentiment,graph,streaming,nosql,performance}"
