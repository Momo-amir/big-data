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

# ── Step 2: init HDFS dirs (idempotent) ──────────────────────────────────────
# ÆNDRET: kør altid init-hdfs.sh. Scriptet bruger mkdir -p og chmod, så det er
# sikkert at køre igen, og nye nødvendige dirs som /tmp/hive bliver ikke sprunget
# over bare fordi /data/Input_dir allerede findes fra en tidligere kørsel.

log "Ensuring HDFS directories and permissions..."
bash "$(dirname "$0")/init-hdfs.sh"

# ── Step 3: wait for hive-metastore ──────────────────────────────────────────
# ÆNDRET: hive-metastore skal være healthy før batch/stream, fordi Spark skriver
# til Hive via metastore. HiveServer2 skal derimod kun bruges til Beeline-queryen
# i run_hive(), så en global wait her blokerer unødigt batch/stream modes.

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

  # Clear checkpoint and Input_dir so the stream sees the next extract as a new file.
  # Structured Streaming skips files it has already processed (tracked by checkpoint),
  # so without this a re-run after batch would never trigger.
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/stream
  docker exec namenode hdfs dfs -rm -f /data/Input_dir/iris.csv

  log "Starting Kafka consumer in background (logs → /tmp/consumer.log)..."
  docker exec -d etl-runner sh -c "python consumer.py > /tmp/consumer.log 2>&1"

  log "Starting stream watcher in background (logs → /tmp/stream.log)..."
  docker exec -d etl-runner sh -c "python -m modules.transform_stream > /tmp/stream.log 2>&1"

  sleep 5
  log "Triggering extract (stream will auto-process)..."
  docker exec etl-runner python extract_only.py

  log "Streaming pipeline running. Follow logs with:"
  echo "  docker exec etl-runner tail -f /tmp/consumer.log"
  echo "  docker exec etl-runner tail -f /tmp/stream.log"
}

run_hive() {
  # ÆNDRET: HiveServer2 ventes kun her, fordi kun Beeline-queryen kræver port 10000.
  # Batch- og stream-pipelines bruger metastore direkte og skal ikke blokeres af
  # en langsom HiveServer2-opstart.
  wait_healthy hive-server
  log "Querying Hive..."
  docker exec hive-server beeline -n hive -u jdbc:hive2://hive-server:10000 \
    -e "SELECT * FROM irisdb.iris_setosa LIMIT 5;"
}

show_consumer() {
  log "Running Kafka consumer (live, Ctrl+C to stop)..."
  docker exec -it etl-runner python consumer.py
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
  consumer)
    show_consumer
    ;;
  *)
    echo "Unknown mode: $MODE"
    echo "Usage: $0 [all|batch|stream|hive|consumer]"
    exit 1
    ;;
esac
