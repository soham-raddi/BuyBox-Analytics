#!/usr/bin/env bash
# Step-by-step live demo from scratch (the automated version of DEMO.md).
# Shows every command before running it and pauses between steps so you can explain.
#   bash scripts/live_demo.sh          full dataset (~17 min)
#   bash scripts/live_demo.sh quick    Spark uses a 20% sample (~10 min)
# NO_BROWSER=1 skips opening the web pages; NO_PAUSE=1 runs straight through.
cd "$(dirname "$0")/.." || exit 1

G='\033[1;32m'; C='\033[1;36m'; Y='\033[1;33m'; B='\033[1m'; R='\033[1;31m'; N='\033[0m'
say()   { echo -e "${Y}$*${N}"; }
cmd()   { echo -e "\n${G}hadoop@buybox:~\$${N} ${B}$1${N}"; eval "$1"; }
step()  { echo; echo -e "${C}==================== $* ====================${N}"; }
pause() { [ -n "${NO_PAUSE:-}" ] && return; echo; read -rp "$(echo -e "${C}>> Press Enter for the next step...${N}")" _; }
open_url() { [ -n "${NO_BROWSER:-}" ] && return; explorer.exe "$1" > /dev/null 2>&1 || true; }

spark_step() {   # run a pipeline step, opening the Spark UI the first time it is up
  echo -e "\n${G}hadoop@buybox:~\$${N} ${B}python run_pipeline.py --steps $1${N}"
  python run_pipeline.py --steps "$1" &
  local pid=$!
  if [ -z "${SPARK_UI_OPENED:-}" ]; then
    for _ in $(seq 1 90); do
      curl -s -o /dev/null http://localhost:4040 && { open_url "http://localhost:4040"; SPARK_UI_OPENED=1; break; }
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
  fi
  wait "$pid"
}

clear
echo -e "${C}"
echo "  ######################################################"
echo "     BuyBox Analytics: live Big Data demo from scratch"
echo "     HDFS -> MapReduce -> Spark -> MLlib -> Streaming -> Cassandra"
echo "  ######################################################"
echo -e "${N}"
[ "${1:-}" = "quick" ] && say "Quick mode: Spark steps use a 20% sample of the data."
echo -e "${R}This wipes HDFS, Cassandra and the generated results, then rebuilds everything live.${N}"
if [ -z "${NO_PAUSE:-}" ]; then
  read -rp "Continue? [y/N] " ok
  [[ "$ok" =~ ^[Yy]$ ]] || { echo "Cancelled."; exit 0; }
fi
T0=$(date +%s)

step "0. Clean slate"
bash scripts/reset_demo.sh > /dev/null 2>&1
say "HDFS, Cassandra and old results cleared. Only the raw dataset in data/raw is kept."

step "1. Environment"
cmd 'source scripts/env.sh'
cmd 'java -version 2>&1 | head -1'
cmd 'hadoop version | head -1'
cmd "python -c \"import pyspark; print('Spark', pyspark.__version__)\""
pause

step "2. Format and start HDFS"
cmd 'HADOOP_ROOT_LOGGER=INFO,console hdfs namenode -format -nonInteractive 2>&1 | grep -i "formatted"'
cmd 'hdfs --daemon start namenode'
cmd 'hdfs --daemon start datanode'
cmd 'jps'
cmd 'bash scripts/wait_for_hdfs.sh'
open_url "http://localhost:9870"
say "HDFS web UI opened: http://localhost:9870  (Overview, Datanodes)"
pause

step "3. Load the dataset into HDFS"
cmd 'ls -lh data/raw'
cmd 'hdfs dfs -mkdir -p /reviewlens/raw'
cmd 'hdfs dfs -put data/raw/*.jsonl /reviewlens/raw/'
cmd 'hdfs dfs -ls -h /reviewlens/raw'
cmd 'hdfs dfs -stat "replication=%r  blocksize=%o  size=%b" /reviewlens/raw/All_Beauty.jsonl'
cmd 'hdfs fsck /reviewlens/raw/All_Beauty.jsonl -files -blocks | grep -E "blk_|Status"'
cmd 'hdfs dfs -cat /reviewlens/raw/All_Beauty.jsonl | head -1'
say "In the HDFS UI: Utilities > Browse the file system > /reviewlens/raw"
pause

step "4. MapReduce on HDFS (Hadoop Streaming)"
cmd 'tail -12 mapreduce/wordcount_mapper.py'
cmd 'bash mapreduce/run_hadoop.sh'
pause

step "5. Start Cassandra"
cmd 'bash scripts/start_cassandra.sh'
cmd 'cqlsh -e "DESCRIBE KEYSPACES;"'
pause

step "6. Spark pipeline (reads the data from HDFS)"
cmd 'export RL_RAW_URI=hdfs://localhost:9000/reviewlens/raw'
[ "${1:-}" = "quick" ] && cmd 'export RL_SAMPLE=0.2'
say "The Spark UI (http://localhost:4040) opens when the first Spark job starts. Refresh it for later steps."
spark_step ingest,analytics
cmd 'hdfs dfs -ls /reviewlens/curated/reviews | head -6'
cmd 'hdfs dfs -du -h /reviewlens'
pause
spark_step sentiment,graph
pause
spark_step streaming
pause
spark_step nosql
cmd 'cqlsh -e "SELECT parent_asin, brand, n_reviews, avg_rating FROM reviewlens.product_summary LIMIT 5;"'
pause
spark_step performance

step "Done in $(( ($(date +%s) - T0) / 60 )) min $(( ($(date +%s) - T0) % 60 )) s"
say "HDFS (http://localhost:9870) and Cassandra are still running for questions."
if [ -z "${NO_PAUSE:-}" ]; then
  read -rp "$(echo -e "${C}>> Press Enter to stop Hadoop and Cassandra and open the dashboard...${N}")" _
fi
bash scripts/stop_services.sh > /dev/null 2>&1
say "Services stopped."
