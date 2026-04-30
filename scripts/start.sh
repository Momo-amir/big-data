#!/usr/bin/env bash
# Full cluster startup + ETL pipeline runner.
# Usage:
#   bash scripts/start.sh          — start cluster + run batch pipeline
#   bash scripts/start.sh stream   — start cluster + run streaming pipeline
#   bash scripts/start.sh batch    — same as default
#   bash scripts/start.sh hive     — query Hive after pipeline has run

set -e

MODE="${1:-batch}"

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

# ── Step 1: start the cluster ─────────────────────────────────────────────────

log "Starting cluster..."
docker compose up -d

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

# ── Step 2: init HDFS dirs (idempotent) ──────────────────────────────────────

log "Initialising HDFS directories..."
bash "$(dirname "$0")/init-hdfs.sh"

# ── Step 3: wait for hive-metastore ──────────────────────────────────────────

wait_healthy hive-metastore

# ── Step 4: run the pipeline ──────────────────────────────────────────────────

case "$MODE" in

  batch)
    log "Running batch pipeline..."
    docker exec etl-runner python main.py
    log "Batch pipeline complete. Output:"
    docker exec namenode hdfs dfs -ls /data/Output_dir
    ;;

  stream)
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
    ;;

  hive)
    log "Querying Hive..."
    docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
      -e "SELECT * FROM irisdb.iris_setosa LIMIT 5;"
    ;;

  *)
    echo "Unknown mode: $MODE"
    echo "Usage: $0 [batch|stream|hive]"
    exit 1
    ;;

esac
