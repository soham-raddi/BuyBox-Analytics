#!/usr/bin/env bash
# Start a single-node HDFS (NameNode + DataNode). Pass "yarn" to also start ResourceManager + NodeManager.
#   bash scripts/start_hadoop.sh [yarn]
set -euo pipefail
source "$(dirname "$0")/env.sh"

if [ ! -d "$HOME/hadoop-data/namenode/current" ]; then
  echo "Formatting NameNode (first run only) ..."
  hdfs namenode -format -nonInteractive -force > "$HADOOP_LOG_DIR/format.log" 2>&1
fi
# setsid detaches the daemons from this shell so they survive after the WSL command returns
setsid hdfs --daemon start namenode < /dev/null || true
setsid hdfs --daemon start datanode < /dev/null || true
if [ "${1:-}" = "yarn" ]; then
  setsid yarn --daemon start resourcemanager < /dev/null || true
  setsid yarn --daemon start nodemanager < /dev/null || true
fi
echo -n "Waiting for HDFS to leave safe mode "
for _ in $(seq 1 30); do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; then echo " ready."; break; fi
  echo -n "."; sleep 2
done
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager" || true
echo "NameNode web UI: http://localhost:9870"
