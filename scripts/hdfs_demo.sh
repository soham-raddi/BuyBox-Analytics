#!/usr/bin/env bash
# Live HDFS demo: starts HDFS, then runs the HDFS commands one by one, printing each command before its output.
# Ends in an interactive shell (Hadoop already on PATH) so more commands can be typed during the demo.
#   bash scripts/hdfs_demo.sh
source "$(dirname "$0")/env.sh"

show() {   # print the command like a prompt, then run it (Hadoop's WARN lines hidden)
  echo
  echo -e "\033[1;32mhadoop@buybox:~\$\033[0m \033[1m$1\033[0m"
  eval "$1" 2>&1 | grep -v -E "WARN|SLF4J"
  sleep 1
}

echo -e "\033[1;36m===== 1. Start HDFS (NameNode + DataNode) =====\033[0m"
bash scripts/start_hadoop.sh 2>&1 | grep -v -E "WARN|log4j"
[ -z "${NO_BROWSER:-}" ] && explorer.exe "http://localhost:9870" > /dev/null 2>&1
show "jps"

echo; echo -e "\033[1;36m===== 2. Files in HDFS =====\033[0m"
show "hdfs dfs -ls -h /reviewlens/raw"
show "hdfs dfs -ls /reviewlens/curated/reviews | head -8"
show "hdfs dfs -du -h /reviewlens"

echo; echo -e "\033[1;36m===== 3. Blocks, replication and health =====\033[0m"
show "hdfs dfs -stat 'name=%n  replication=%r  blocksize=%o  size=%b' /reviewlens/raw/*"
show "hdfs fsck /reviewlens/raw/All_Beauty.jsonl -files -blocks | grep -E 'block|Status|Total'"

echo; echo -e "\033[1;36m===== 4. Read data straight from HDFS =====\033[0m"
show "hdfs dfs -cat /reviewlens/raw/All_Beauty.jsonl | head -2"
show "hdfs dfs -cat /reviewlens/mapreduce/complaint_wordcount/part-* | sort -t\$'\t' -k2,2nr | head -5"

echo; echo -e "\033[1;36m===== 5. Cluster report =====\033[0m"
show "hdfs dfsadmin -report | head -12"

echo
echo -e "\033[1;33mHDFS web UI: http://localhost:9870   (Utilities > Browse the file system > /reviewlens)\033[0m"
echo -e "\033[1;33mYou are now in a WSL shell with Hadoop on the PATH. Try: hdfs dfs -ls /reviewlens   (type exit to leave)\033[0m"
exec bash -i
