#!/usr/bin/env bash
# Start a single-node Apache Cassandra in the background (data in ~/cassandra-data, 1 GB heap).
set -euo pipefail
source "$(dirname "$0")/env.sh"
DATA="$HOME/cassandra-data"
mkdir -p "$DATA"
if [ -f "$DATA/cassandra.pid" ] && kill -0 "$(cat "$DATA/cassandra.pid")" 2>/dev/null; then
  echo "Cassandra already running (pid $(cat "$DATA/cassandra.pid"))"; exit 0
fi
export MAX_HEAP_SIZE=1536M HEAP_NEWSIZE=384M
export CASSANDRA_LOG_DIR="$DATA/logs"
JVM_EXTRA_OPTS="-Dcassandra.storagedir=$DATA" setsid cassandra -p "$DATA/cassandra.pid" > "$DATA/startup.log" 2>&1
echo -n "Waiting for Cassandra on port 9042 "
for _ in $(seq 1 60); do
  if (exec 3<>/dev/tcp/127.0.0.1/9042) 2>/dev/null; then echo " ready."; exit 0; fi
  echo -n "."; sleep 2
done
echo " timed out - see $DATA/logs/system.log"; exit 1
