#!/usr/bin/env bash
# End-to-end run of the BuyBox Analytics project inside WSL/Linux:
#   HDFS -> MapReduce -> Spark pipeline (reading from HDFS) -> Cassandra -> outputs for the dashboard
#   bash run_all.sh            full run
#   RL_SAMPLE=0.2 bash run_all.sh   quick run on a 20% sample
set -euo pipefail
source "$(dirname "$0")/scripts/env.sh"

echo "##### 1. Start HDFS and load the raw data #####"
bash scripts/start_hadoop.sh
bash scripts/hdfs_load.sh

echo "##### 2. Hadoop Streaming MapReduce jobs #####"
bash mapreduce/run_hadoop.sh

echo "##### 3. Start Cassandra #####"
bash scripts/start_cassandra.sh || echo "Cassandra did not start - the nosql step will be skipped"

echo "##### 4. Spark pipeline (input read from HDFS) #####"
RL_RAW_URI=hdfs://localhost:9000/reviewlens/raw python run_pipeline.py 2>&1 | grep -v -E "^\s+at |WARN"

echo
echo "All done. Open the dashboard from Windows:   streamlit run dashboard/app.py"
echo "Stop services with:                          bash scripts/stop_services.sh"
