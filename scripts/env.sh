# Source this file inside WSL/Linux:  source scripts/env.sh
# Sets up Java, Python venv, Hadoop and Spark environment variables for the project.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_DIR

export JAVA_HOME="${JAVA_HOME_17:-$HOME/opt/jdk17}"
# Hadoop: an explicit HADOOP_HOME_LINUX wins, then ~/opt/hadoop (installed by setup_wsl.sh), then C:\hadoop
if [ -n "${HADOOP_HOME_LINUX:-}" ]; then export HADOOP_HOME="$HADOOP_HOME_LINUX"
elif [ -x "$HOME/opt/hadoop/bin/hadoop" ]; then export HADOOP_HOME="$HOME/opt/hadoop"
else export HADOOP_HOME="/mnt/c/hadoop"; fi
export HADOOP_CONF_DIR="$PROJECT_DIR/hadoop/conf"
export HADOOP_LOG_DIR="$HOME/hadoop-data/logs"
export HADOOP_PID_DIR="$HOME/hadoop-data/pids"
export CASSANDRA_HOME="${CASSANDRA_HOME:-$HOME/opt/cassandra}"
# Java 17 needs these to let Hadoop 3.3 use internal JDK classes
export HADOOP_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$CASSANDRA_HOME/bin:$PATH"
# Client commands (hdfs dfs, hadoop jar ...) print only errors; daemons keep their own INFO logs in HADOOP_LOG_DIR
export HADOOP_ROOT_LOGGER="${HADOOP_ROOT_LOGGER:-ERROR,console}"

VENV="${RL_VENV:-$HOME/.venvs/reviewlens}"
# shellcheck disable=SC1091
[ -f "$VENV/bin/activate" ] && source "$VENV/bin/activate"
export PYSPARK_PYTHON="$VENV/bin/python"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
cd "$PROJECT_DIR" || return
