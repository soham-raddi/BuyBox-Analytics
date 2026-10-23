#!/usr/bin/env bash
# Waits until the NameNode accepts connections (it needs ~10-20 s after "hdfs --daemon start namenode"),
# then until HDFS has left safe mode.
#   bash scripts/wait_for_hdfs.sh
source "$(dirname "$0")/env.sh" > /dev/null

missing=""
jps | grep -qw NameNode || missing="namenode"
jps | grep -qw DataNode || missing="$missing datanode"
if [ -n "$missing" ]; then
  echo "Not running:$(for d in $missing; do printf ' %s' "$( [ "$d" = namenode ] && echo NameNode || echo DataNode )"; done)"
  for d in $missing; do echo "  Start it with:  hdfs --daemon start $d"; done
  echo "Then run this script again."
  exit 1
fi

echo -n "Waiting for the NameNode to start "
for _ in $(seq 1 60); do
  if hdfs dfsadmin -safemode get > /dev/null 2>&1; then echo " up."; break; fi
  if ! jps | grep -qw NameNode; then
    echo; echo "The NameNode stopped. See its log:  tail -50 \$HADOOP_LOG_DIR/*namenode*.log"; exit 1
  fi
  echo -n "."; sleep 2
done
hdfs dfsadmin -safemode wait
