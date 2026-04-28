# ETL Architecture — Extract, Transform, Load

## What ETL is and why it exists as a pattern

ETL is not a technology. It is a way of thinking about data movement that separates three concerns which have very different requirements:

**Extract** — get data from somewhere. The challenge here is network reliability, authentication, rate limits, and format parsing. The extract step should do nothing except fetch raw data faithfully and store it. No business logic.

**Transform** — reshape the data into what you actually need. Filter rows, rename columns, clean nulls, change units, join with other datasets. This is where business logic lives. It should operate on data that is already stored — not on a live network connection.

**Load** — write the transformed data to its destination. Choose the format, the location, handle overwrites.

The reason to separate these is that each step fails in different ways and needs to be retried independently. If your transform crashes, you should not have to re-download the data. If the load fails, you should not have to re-run the transform. Each step takes stable input and produces stable output.

---

## Batch vs Stream — the same ETL, different triggers

Both pipelines in this project implement ETL. The difference is only in how and when transform/load is triggered.

### Batch

```
main.py runs
    │
    ├── 1. Extract  → downloads iris.csv → writes to HDFS Input_dir
    │
    ├── 2. Transform → Spark reads from Input_dir → filters Iris-setosa
    │
    └── 3. Load     → Spark writes to HDFS Output_dir → done
```

This is a linear, synchronous pipeline. One run, one result. You control when it runs.

Use batch when: the data source is static or updated infrequently, you want to process a complete snapshot, or timing is not critical.

### Stream

```
transform_stream.py starts (Spark background process)
    │
    └── watches HDFS Input_dir forever...
              │
              │  (later, separately)
              │
    extract.py runs
              │
              └── drops new file in Input_dir
                        │
                        ▼
              Spark detects new file
                        │
                        └── Transform + Load runs automatically
```

Transform and load are now **decoupled from extract in time**. Extract is an independent script. Spark is a permanent background process. They communicate only through the filesystem — extract writes files, Spark reacts to them.

Use streaming when: data arrives unpredictably, you need low latency between data arrival and processing, or the extract step is owned by a separate system.

---

## Why the filesystem is the interface between extract and stream

You could imagine other ways to connect extract to transform: a message queue, a REST API, a shared database. This project uses HDFS as the interface — extract writes a file, Spark watches for files.

This is not just simplicity. It is a well-understood pattern called **file-based integration**. The properties are:

- **Decoupled** — extract does not need to know Spark exists. Spark does not care how the file got there.
- **Buffered** — if Spark is down when extract runs, the file waits. When Spark comes back up, it processes it. No data is lost.
- **Auditable** — the files in `Input_dir` are a log of everything that was ever extracted. You can reprocess them.
- **Simple failure recovery** — if transform crashes halfway through, the input file is still there. Re-run transform.

A message queue (Kafka, RabbitMQ) gives lower latency and more sophisticated routing but adds significant operational complexity. For this use case — a handful of CSV files from a research dataset — files are the right tool.

---

## Module boundaries — why the code is split this way

```
src/
├── main.py              ← orchestrator: calls extract, then submits Spark job
├── submit_jobs.py       ← infrastructure: wraps spark-submit
├── modules/
│   ├── extract.py       ← ETL phase 1: network → HDFS
│   ├── transform_batch.py   ← ETL phase 2+3 (batch): HDFS → filter → HDFS
│   └── transform_stream.py  ← ETL phase 2+3 (stream): watch → filter → HDFS
└── utils/
    ├── hdfs_client.py   ← shared: HDFS connection and write helpers
    └── validation.py    ← shared: URL and input validation
```

`modules/` are the ETL phases — they contain your business logic. Each one has a single responsibility and could be tested or replaced independently.

`utils/` are shared infrastructure — things that are needed by more than one module. `hdfs_client.py` is used by extract to write and by the transform modules to read. `validation.py` is used by extract to check the source URL.

`main.py` orchestrates but contains no logic of its own. It calls extract, then calls submit_jobs to fire the Spark job. If you wanted to change the pipeline order or add a new step, this is the only file you touch.

`submit_jobs.py` is separate from `main.py` because submitting a Spark job is an infrastructure concern, not a business logic concern. It also needs to be callable independently for the stream mode, where you start the background job before running extract.

---

## What "load" means in this project

In classic ETL, "load" is often a separate step — writing to a database, an API, a warehouse. In this project, load is embedded in the transform scripts: the final `.write.csv(output_path)` call is the load step.

This is fine. The conceptual separation still holds — transform produces a dataframe, load writes it. They just happen to be in the same file. If you ever needed to change the output format (CSV → Parquet, HDFS → S3), you would change only the load portion without touching the filter logic.
