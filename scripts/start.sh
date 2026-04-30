#!/usr/bin/env bash
# Full cluster startup + ETL pipeline runner.
# Usage:
#   bash scripts/start.sh          — start cluster + run ALL pipelines
#   bash scripts/start.sh batch    — start cluster + batch only
#   bash scripts/start.sh stream   — start cluster + streaming only
#   bash scripts/start.sh hive     — query Hive only

set -e

MODE="${1:-all}"

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { echo ""; echo "==> $*"; }
wait_healthy() {
  local name="$1"
  local max=60
  local i=0
  log "Waiting for $name to be healthy..."
  until [ "$(docker inspect "$name" --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do
    i=$((i+1))
    if [ $i -ge $max ]; then
      echo "ERROR: $name did not become healthy in time."
      docker compose logs "$name" | tail -20
      exit 1
    fi
    printf "."
    sleep 5
  done
  echo " healthy."
}

# ── Step 1: start the cluster (skip if already up) ───────────────────────────

if [ "$(docker inspect namenode --format '{{.State.Running}}' 2>/dev/null)" = "true" ]; then
  log "Cluster already running, skipping docker compose up."
else
  log "Starting cluster..."
  docker compose up -d
fi

wait_healthy namenode
wait_healthy mysql

# Kafka can fail on first start if Zookeeper has a stale broker node — restart both if needed
if ! wait_healthy kafka 2>/dev/null; then
  log "Kafka unhealthy, restarting zookeeper + kafka..."
  docker compose restart zookeeper
  sleep 5
  docker compose restart kafka
fi
wait_healthy kafka

# ── Step 2: init HDFS dirs (once only) ───────────────────────────────────────

if docker exec namenode hdfs dfs -test -d /data/Input_dir 2>/dev/null; then
  log "HDFS directories already exist, skipping init."
else
  log "Initialising HDFS directories..."
  bash "$(dirname "$0")/init-hdfs.sh"
fi

# ── Step 3: wait for hive-metastore ──────────────────────────────────────────

wait_healthy hive-metastore

# ── Step 4: run the pipeline ──────────────────────────────────────────────────

run_batch() {
  log "Running batch pipeline..."
  docker exec etl-runner python main.py
  log "Batch pipeline complete. Output:"
  docker exec namenode hdfs dfs -ls /data/Output_dir
}

run_stream() {
  log "Starting streaming pipeline..."
  log "Starting Kafka consumer in background (logs → /tmp/consumer.log)..."
  docker exec -d etl-runner sh -c "python consumer.py > /tmp/consumer.log 2>&1"

  log "Starting stream watcher in background (logs → /tmp/stream.log)..."
  docker exec -d etl-runner sh -c "python -m modules.transform_stream > /tmp/stream.log 2>&1"

  sleep 3
  log "Triggering extract (stream will auto-process)..."
  docker exec etl-runner python extract_only.py

  log "Streaming pipeline running. Follow logs with:"
  echo "  docker exec etl-runner tail -f /tmp/consumer.log"
  echo "  docker exec etl-runner tail -f /tmp/stream.log"
}

run_hive() {
  log "Querying Hive..."
  docker exec hive-server beeline -u jdbc:hive2://hive-server:10000 \
    -e "SELECT * FROM irisdb.iris_setosa LIMIT 5;"
}

case "$MODE" in
  all)
    run_batch
    run_stream
    run_hive
    ;;
  batch)
    run_batch
    ;;
  stream)
    run_stream
    ;;
  hive)
    run_hive
    ;;
  *)
    echo "Unknown mode: $MODE"
    echo "Usage: $0 [all|batch|stream|hive]"
    exit 1
    ;;
esac
