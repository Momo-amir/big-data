#!/usr/bin/env bash
# Run once after the cluster is up to create the HDFS directory structure.
# Usage: bash scripts/init-hdfs.sh

set -e

NAMENODE=namenode

echo "Waiting for HDFS to leave safe mode..."
docker exec $NAMENODE hdfs dfsadmin -safemode wait

echo "Creating HDFS directories..."
docker exec $NAMENODE hdfs dfs -mkdir -p /data/Input_dir
docker exec $NAMENODE hdfs dfs -mkdir -p /data/Output_dir
docker exec $NAMENODE hdfs dfs -mkdir -p /checkpoints/stream
docker exec $NAMENODE hdfs dfs -mkdir -p /spark-logs
docker exec $NAMENODE hdfs dfs -mkdir -p /user/hive/warehouse
docker exec $NAMENODE hdfs dfs -mkdir -p /tmp/hive
docker exec $NAMENODE hdfs dfs -mkdir -p /tmp/hive-scratch

docker exec $NAMENODE hdfs dfs -chmod -R 777 /data
docker exec $NAMENODE hdfs dfs -chmod -R 777 /checkpoints
docker exec $NAMENODE hdfs dfs -chmod -R 777 /spark-logs
docker exec $NAMENODE hdfs dfs -chmod -R 777 /user
docker exec $NAMENODE hdfs dfs -chmod -R 777 /tmp

echo "HDFS directory structure ready."
docker exec $NAMENODE hdfs dfs -ls /data
