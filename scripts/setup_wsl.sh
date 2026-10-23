#!/usr/bin/env bash
# One-time setup inside WSL Ubuntu / Linux. Needs no sudo: everything goes into ~/opt and ~/.venvs.
#   bash scripts/setup_wsl.sh
set -euo pipefail
mkdir -p ~/opt ~/.venvs
HERE="$(cd "$(dirname "$0")/.." && pwd)"

if [ ! -x ~/opt/jdk17/bin/java ]; then
  echo "Installing Microsoft Build of OpenJDK 17 ..."
  curl -sSL -o /tmp/jdk17.tar.gz "https://aka.ms/download-jdk/microsoft-jdk-17-linux-x64.tar.gz"
  mkdir -p ~/opt/jdk17 && tar -xzf /tmp/jdk17.tar.gz -C ~/opt/jdk17 --strip-components=1 && rm /tmp/jdk17.tar.gz
fi

if [ ! -x ~/.venvs/reviewlens/bin/pip ]; then
  echo "Creating Python venv ..."
  if python3 -m venv ~/.venvs/reviewlens 2>/dev/null; then :; else
    rm -rf ~/.venvs/reviewlens
    python3 -m venv --without-pip ~/.venvs/reviewlens          # Ubuntu without python3-venv
    curl -sSL -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py
    ~/.venvs/reviewlens/bin/python /tmp/get-pip.py -q
  fi
fi
~/.venvs/reviewlens/bin/pip install -q -r "$HERE/requirements.txt"

CASS_VER=5.0.9
if [ ! -x ~/opt/cassandra/bin/cassandra ]; then
  echo "Installing Apache Cassandra $CASS_VER ..."
  # dlcdn only keeps the newest releases; fall back to the permanent Apache archive
  curl -fsSL -o /tmp/cassandra.tar.gz "https://dlcdn.apache.org/cassandra/$CASS_VER/apache-cassandra-$CASS_VER-bin.tar.gz" ||
    curl -fsSL -o /tmp/cassandra.tar.gz "https://archive.apache.org/dist/cassandra/$CASS_VER/apache-cassandra-$CASS_VER-bin.tar.gz"
  mkdir -p ~/opt/cassandra && tar -xzf /tmp/cassandra.tar.gz -C ~/opt/cassandra --strip-components=1 && rm /tmp/cassandra.tar.gz
fi

# Hadoop: reuse a Windows install at C:\hadoop if there is one, otherwise download 3.3.6 into ~/opt/hadoop
# (scripts/env.sh looks in both places)
if [ ! -x "${HADOOP_HOME_LINUX:-/mnt/c/hadoop}/bin/hadoop" ] && [ ! -x ~/opt/hadoop/bin/hadoop ]; then
  echo "Installing Apache Hadoop 3.3.6 to ~/opt/hadoop (about 700 MB) ..."
  curl -fsSL -o /tmp/hadoop.tar.gz "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
  mkdir -p ~/opt/hadoop && tar -xzf /tmp/hadoop.tar.gz -C ~/opt/hadoop --strip-components=1 && rm /tmp/hadoop.tar.gz
fi

bash "$HERE/scripts/download_data.sh"
echo "Setup complete. Next:  bash run_all.sh"
