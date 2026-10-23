#!/usr/bin/env bash
# Resets everything so the project can be executed from scratch in front of an audience:
#   - stops HDFS, YARN and Cassandra
#   - deletes all HDFS storage (NameNode metadata + DataNode blocks) -> HDFS must be formatted again
#   - deletes all Cassandra data
#   - deletes generated results in outputs/ and the streaming folders
# The dataset in data/raw/ is kept. Results can always be restored with:  git checkout -- outputs
#   bash scripts/reset_demo.sh
source "$(dirname "$0")/env.sh"

echo "Stopping services ..."
bash scripts/stop_services.sh > /dev/null 2>&1
pkill -f org.apache.hadoop 2>/dev/null
pkill -f CassandraDaemon 2>/dev/null
pkill -f spark_demo.py 2>/dev/null
pkill -f org.apache.spark.deploy.SparkSubmit 2>/dev/null
sleep 2

echo "Wiping HDFS storage (~/hadoop-data) and Cassandra data (~/cassandra-data) ..."
rm -rf "$HOME/hadoop-data" "$HOME/cassandra-data"

echo "Clearing generated results ..."
# keep the architecture diagram and HDFS report: the pipeline does not regenerate them
find outputs/figures -type f ! -name "00_architecture.png" -delete 2>/dev/null
rm -rf outputs/tables outputs/summary.json outputs/*.log data/stream
mkdir -p outputs/figures

echo
echo "Clean slate. Dataset kept in data/raw:"
ls -lh data/raw
echo
echo "Now follow DEMO.md from step 1."
