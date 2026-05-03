# Assignment Completion — TLDR Proof Guide

## Assignment checklist

### Iteration 1 — Batch ETL Pipeline

- [x] **Extract module** (`src/modules/extract.py`) — downloads iris.csv via `requests` (HTTPS-only, chunked streaming, no subprocess/shell, direct to HDFS via WebHDFS)
- [x] **HTTPS enforced** — `extract()` raises `ValueError` if URL does not start with `https://`
- [x] **No command-injection** — pure Python `requests`, no `subprocess(shell=True)`
- [x] **Chunked download** — `iter_content(chunk_size=8192)`, file never fully in memory
- [x] **Column headers added** — `sepal_length, sepal_width, petal_length, petal_width, species` prepended before writing
- [x] **Dynamic filename** — filename derived from URL (`url.split("/")[-1]`), never hardcoded
- [x] **Transform module** (`src/modules/transform_batch.py`) — PySpark on remote Spark master, filters `species == "Iris-setosa"`
- [x] **Load module** (`src/modules/load.py`) — saves `transformed_iris.csv` to HDFS Output_dir
- [x] **Batch main** (`src/main.py`) — orchestrates Extract → Transform → Load

### Iteration 2 — Hive + Encryption + Visualization

- [x] **Apache Hive integration** — `save_to_hive()` in `load.py`, auto-creates `irisdb` database if missing, writes `iris_setosa` table
- [x] **AES-256-GCM encryption** (`src/modules/security.py`) — chosen over CBC because GCM provides authentication (AEAD), preventing tampered ciphertext from being silently decrypted; random 12-byte nonce per encryption (IND-CPA secure)
- [x] **CSV encrypted** — every cell encrypted before writing to HDFS
- [x] **Hive table encrypted** — every cell encrypted before saving to Hive
- [x] **Visualization module** (`src/modules/visualization.py`) — reads from Hive, decrypts, generates 3 diagrams saved to HDFS Output_dir
  - [x] Scatter plot: `sepal_length` (x) vs `petal_length` (y)
  - [x] Histogram: `petal_width` with 10 bins
  - [x] Boxplot: 2×2 layout with all four measurements
- [x] **Real-time pipeline** (`src/modules/transform_stream.py`) — Spark Structured Streaming monitors HDFS Input_dir, auto-triggers T+L+Viz+Kafka on new files

### Iteration 3 — Kafka Streaming

- [x] **Kafka producer** (`src/modules/kafka_producer.py`) — reads encrypted Hive table, decrypts, publishes JSON rows to `iris-setosa` topic
- [x] **Kafka consumer** (`src/consumer.py`) — standalone script, subscribes and prints records to console
- [x] **Decoupled extract** (`src/extract_only.py`) — drops file into HDFS Input_dir; background stream pipeline picks it up automatically

---

## How to prove each requirement at demo time

### 1 — Extract writes directly to HDFS (no local file)

```bash
# Run everything with the command below (starts all containers, including Spark, Hive, Kafka)
bash scripts/start.sh

# Run extract inside the container
docker exec etl-runner python extract_only.py

# Verify file exists in HDFS Input_dir
docker exec namenode hdfs dfs -ls /data/Input_dir

# Print the first few lines (shows headers + raw data)
docker exec namenode hdfs dfs -cat /data/Input_dir/iris.csv | head -5
```

Expected output: `sepal_length,sepal_width,petal_length,petal_width,species` as first line.

---

### 2 — Batch ETL transforms and loads

```bash
docker exec etl-runner python main.py

# Check Output_dir for encrypted CSV
docker exec namenode hdfs dfs -ls /data/Output_dir
docker exec namenode hdfs dfs -cat /data/Output_dir/transformed_iris.csv | head -3
```

Expected: base64-encoded ciphertext per cell (not readable plaintext).

---

### 3 — Encrypted data in Hive (view with Beeline)

```bash
docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000"
```

Inside Beeline:

```sql
USE irisdb;
SELECT * FROM iris_setosa LIMIT 5;
```

Expected: all values are base64 ciphertext strings, not plaintext numbers/species names.

---

### 4 — Real-time pipeline auto-triggers on new file

```bash
# Terminal 1 — start background streaming pipeline
docker exec etl-runner python -m modules.transform_stream

# Terminal 2 — drop a new file (decoupled extract)
docker exec etl-runner python extract_only.py
```

Watch Terminal 1 log: `[stream] Processing batch 0 ... N Iris-setosa rows — running Load, Viz, Kafka ...`

---

### 5 — Diagrams generated and stored on HDFS

```bash
docker exec namenode hdfs dfs -ls /data/Output_dir
```

Expected files: `scatter_plot.png`, `histogram.png`, `boxplot.png`

To view locally:

```bash
docker exec namenode hdfs dfs -get /data/Output_dir/scatter_plot.png /tmp/
docker cp namenode:/tmp/scatter_plot.png .
open scatter_plot.png
```

---

### 6 — Kafka consumer receives live data

```bash
# Terminal 1 — consumer waiting
docker exec etl-runner python consumer.py

# Terminal 2 — trigger the pipeline (or let the stream pipeline handle it)
docker exec etl-runner python main.py
```

Watch Terminal 1 print JSON records with plaintext iris data.

---

### 7 — HDFS navigation (terminal demo)

```bash
# Show directory structure
docker exec namenode hdfs dfs -ls /data/
docker exec namenode hdfs dfs -ls /data/Input_dir
docker exec namenode hdfs dfs -ls /data/Output_dir

# Print file contents
docker exec namenode hdfs dfs -cat /data/Input_dir/iris.csv
docker exec namenode hdfs dfs -cat /data/Output_dir/transformed_iris.csv
```

---

## Security justification (AES-GCM over CBC)

`security.py` uses **AES-256-GCM** because:

- GCM is an **AEAD** cipher — it produces a 16-byte authentication tag that detects any tampering with the ciphertext before decryption.
- CBC provides only confidentiality; a tampered ciphertext decrypts silently to garbage.
- A fresh random 12-byte nonce is generated per `encrypt()` call, so identical plaintexts always produce different ciphertexts (IND-CPA secure).

Wire format: `base64(nonce[12] || tag[16] || ciphertext)`
