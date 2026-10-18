#!/usr/bin/env bash
# Stop Hadoop daemons and Cassandra started by the project scripts.
source "$(dirname "$0")/env.sh"
yarn --daemon stop nodemanager 2>/dev/null
yarn --daemon stop resourcemanager 2>/dev/null
hdfs --daemon stop datanode 2>/dev/null
hdfs --daemon stop namenode 2>/dev/null
if [ -f "$HOME/cassandra-data/cassandra.pid" ]; then
  kill "$(cat "$HOME/cassandra-data/cassandra.pid")" 2>/dev/null && echo "Cassandra stopped"
  rm -f "$HOME/cassandra-data/cassandra.pid"
fi
jps
