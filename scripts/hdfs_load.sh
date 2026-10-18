#!/usr/bin/env bash
# Load the raw dataset into HDFS and show the HDFS management commands (lab assignment 1).
set -euo pipefail
source "$(dirname "$0")/env.sh"
LOG="$PROJECT_DIR/outputs/hdfs_report.txt"
RAW=/reviewlens/raw
{
  echo "### hdfs dfs -mkdir -p $RAW"
  hdfs dfs -mkdir -p $RAW /reviewlens/mapreduce
  echo "### hdfs dfs -put (upload JSON Lines files)"
  for f in data/raw/All_Beauty.jsonl data/raw/meta_All_Beauty.jsonl; do
    hdfs dfs -put -f "$f" $RAW/
  done
  echo "### hdfs dfs -ls -h $RAW"
  hdfs dfs -ls -h $RAW
  echo "### Replication factor, block size and size (hdfs dfs -stat)"
  hdfs dfs -stat "name=%n replication=%r blocksize=%o bytes=%b" $RAW/*
  echo "### hdfs dfs -du -h"
  hdfs dfs -du -h /reviewlens
  echo "### hdfs fsck (blocks and their locations)"
  hdfs fsck $RAW/All_Beauty.jsonl -files -blocks | grep -E "blocks|Total|Status|replicated|^[0-9]\." | head -20
  echo "### hdfs dfs -setrep (change replication factor; single node keeps 1 live copy)"
  hdfs dfs -setrep 1 $RAW/meta_All_Beauty.jsonl
  echo "### hdfs dfs -tail (last KB of the file)"
  hdfs dfs -tail $RAW/All_Beauty.jsonl | tail -c 300; echo
  echo "### hdfs dfs -get (download back to local disk) + checksum comparison"
  rm -f /tmp/hdfs_copy.jsonl
  hdfs dfs -get $RAW/meta_All_Beauty.jsonl /tmp/hdfs_copy.jsonl
  echo "local : $(md5sum < data/raw/meta_All_Beauty.jsonl)"
  echo "hdfs  : $(md5sum < /tmp/hdfs_copy.jsonl)"
  rm -f /tmp/hdfs_copy.jsonl
  echo "### hdfs dfsadmin -report"
  hdfs dfsadmin -report | head -20
} 2>&1 | grep -v -E "WARN|SLF4J" | tee "$LOG"
echo "Saved report to $LOG"
