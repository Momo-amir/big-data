## Big Data ETL Pipeline — Developer Handbook

A hands-on big data pipeline built for a nature centre that needs to collect, filter, and store botanical research data at scale. The assignment uses iris flower data as a stand-in for any large scientific dataset, and the pipeline is designed to handle both small and very large CSV files without choking.

### Background reading

Before writing any code, read these. They explain the _why_ behind every decision in this project:

- [docs/etl-architecture.md](docs/etl-architecture.md) — what ETL is, batch vs stream, why the code is split the way it is
- [docs/hadoop-hdfs.md](docs/hadoop-hdfs.md) — why distributed storage, how HDFS works, why we write directly to it
- [docs/spark.md](docs/spark.md) — why distributed processing, how Spark executes jobs, lazy evaluation, batch vs streaming API
- [docs/security.md](docs/security.md) — the actual threats behind HTTPS enforcement, chunked streaming, and injection prevention

---

### The Problem

A research organisation collects flora data from an international cloud source as CSV files. Some files are tiny, some are massive. They need two things:

1. **Batch ETL** — download a file, filter it, save the result. Start to finish, one run.
2. **Real-time ETL** — the extract step runs independently; the transform/load runs as a background process that automatically kicks in whenever new data lands in the input directory.

Both pipelines must write data directly to a **distributed file system (HDFS)** — never to local disk — and the transform step must use **Apache Spark** to handle data too large to fit in memory.

---

### Why This Stack

| Technology            | Role                      | Why                                                                                                                                                    |
| --------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **HDFS (Hadoop)**     | Distributed file system   | Stores data across multiple nodes. Scales horizontally — add machines, not bigger machines. Single-node in dev, same config in prod.                   |
| **Apache Spark**      | Data processing engine    | Processes data in parallel across the cluster. Handles datasets far larger than RAM. Supports both batch and streaming modes from the same API.        |
| **PySpark**           | Python interface to Spark | Write transformation logic in Python while Spark handles distribution and parallelism under the hood.                                                  |
| **Python `requests`** | HTTP extract              | Streams downloads in chunks so a 10 GB file never fully loads into memory. HTTPS enforced, parameters validated to prevent injection.                  |
| **Docker Compose**    | Local cluster simulation  | Runs namenode, datanode, spark-master, and spark-worker as separate containers on one machine — mirrors a real multi-node cluster without needing one. |

The key architectural insight: a distributed system is just a cluster of ordinary machines coordinated by software. You develop locally against a single-node cluster and deploy to production by changing a URL — `localhost:9000` becomes the production namenode address. Nothing else changes.

---

### Architecture

```
[Source URL]
     │
     ▼
 extract.py          ← streams download in chunks, writes directly to HDFS
     │
     ▼
 HDFS Input_dir      ← raw CSV lives here (never on local disk)
     │
     ├── Batch ──────► transform_batch.py   (PySpark job, runs once)
     │                        │
     └── Stream ─────► transform_stream.py  (PySpark job, watches Input_dir)
                              │
                              ▼
                      HDFS Output_dir       ← filtered CSV: only Iris-setosa rows
```

**Batch mode**: `main.py` orchestrates extract → transform → load in sequence.

**Stream mode**: `transform_stream.py` runs as a persistent background process watching HDFS `Input_dir`. You run `extract.py` separately at any time; Spark detects the new file and processes it automatically.

---

### Project Structure

```
big-data/
├── Dockerfile                # Builds the ETL runner container
├── docker-compose.yml        # Spins up the full cluster (Hadoop + Spark + ETL runner)
├── src/                      # ← everything you write lives here
│   ├── main.py               # TODO: ETL entrypoint (batch pipeline)
│   ├── submit_jobs.py        # TODO: spark-submit wrapper (injection-safe)
│   ├── requirements.txt      # Python dependencies
│   ├── modules/
│   │   ├── extract.py        # TODO: chunked HTTPS download → HDFS
│   │   ├── transform_batch.py    # TODO: batch PySpark job
│   │   └── transform_stream.py   # TODO: streaming PySpark job
│   └── utils/
│       ├── hdfs_client.py    # TODO: HDFS connection + streaming write helpers
│       └── validation.py     # TODO: URL validation + HTTPS enforcement
├── hadoop/config/            # Hadoop XML config — do not edit
├── spark/config/             # Spark defaults — do not edit
└── scripts/
    └── init-hdfs.sh          # Creates Input_dir and Output_dir on HDFS at startup
```

`src/` is live-mounted into the container — edit locally, run immediately, no rebuild needed. Everything outside `src/` is infrastructure config you should not need to touch.

Each module maps directly to one ETL phase. `main.py` is the only entry point you call — it imports and sequences the others.

---

### Data

Source: `https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv`

The raw CSV has no header row. The five columns are:

| Index | Column         | Example     |
| ----- | -------------- | ----------- |
| 0     | `sepal_length` | 5.1         |
| 1     | `sepal_width`  | 3.5         |
| 2     | `petal_length` | 1.4         |
| 3     | `petal_width`  | 0.2         |
| 4     | `species`      | Iris-setosa |

The transform step filters to rows where `species == "Iris-setosa"` and saves them to `Output_dir` as `transformed_iris.csv`.

---

### Security Requirements Baked In

These are not optional — they are part of the assignment spec:

- **HTTPS only** — `validation.py` must reject any URL that is not `https://`
- **No command injection** — `submit_jobs.py` must call `subprocess` with `shell=False` (list form, never a string)
- **Chunked download** — `extract.py` streams the response body in small blocks; never loads the full file into memory
- **Write directly to HDFS** — data must never touch local disk; use the `hdfs` Python client to pipe chunks straight to HDFS

---

### Setup

```bash
# 1. Copy environment config
cp .env.example .env

# 2. Build and start the full cluster (Hadoop + Spark + ETL container)
docker compose up --build -d


# 3. when docker is running, initialize HDFS directories (only needed once)
bash scripts/init-hdfs.sh

# 4. Verify HDFS is working
docker exec namenode hdfs dfs -ls /

# 5. On subsequent starts (no rebuild needed)
docker compose up -d

```

**Verify the cluster is healthy:**

- Hadoop NameNode UI: http://localhost:9870/dfshealth.html#tab-overview
- Spark Master UI: http://localhost:8081/

Wait for the NameNode to leave safe mode before running any jobs (the UI will show this).

---

### Running the Pipeline

**Batch (full ETL in one command):**

```bash
docker exec etl-runner python main.py
```

**Stream mode (two separate terminals):**

```bash
# Terminal 1 — start the background Spark streaming job
docker exec etl-runner python submit_jobs.py --mode stream

# Terminal 2 — run extract whenever you want; Spark picks it up automatically
docker exec etl-runner python extract.py
```

---

### What You Need to Implement

All `TODO` comments mark the work to be done. Suggested order:

1. [src/utils/validation.py](src/utils/validation.py) — URL validation and HTTPS enforcement
2. [src/utils/hdfs_client.py](src/utils/hdfs_client.py) — HDFS connection and chunk-write helpers
3. [src/modules/extract.py](src/modules/extract.py) — chunked download streamed directly to HDFS
4. [src/modules/transform_batch.py](src/modules/transform_batch.py) — PySpark batch filter + CSV write
5. [src/modules/transform_stream.py](src/modules/transform_stream.py) — PySpark structured streaming job
6. [src/submit_jobs.py](src/submit_jobs.py) — injection-safe `spark-submit` wrapper
7. [src/main.py](src/main.py) — orchestrates extract → submit batch job → done
