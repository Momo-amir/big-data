# Apache Spark — Why Distributed Processing?

## The problem it solves

You have a 500 GB CSV on HDFS. You want to filter it. With pandas:

```python
import pandas as pd
df = pd.read_csv("file.csv")  # tries to load 500 GB into RAM
```

Your machine has 16 GB of RAM. It crashes.

You could read it in chunks with pandas, but then you are writing your own distributed processing logic by hand — managing chunks, parallelism, failures, memory. That is what Spark already does, and it does it well.

**Spark is a distributed computation engine.** It takes a job, splits it across the nodes in a cluster, runs everything in parallel, and hands you back the result. From your perspective, you write a transformation on a dataframe and Spark handles the rest.

---

## How Spark actually works

Spark has the same master/worker pattern as HDFS:

**Spark Master** — receives jobs, decides how to split work, assigns tasks to workers.

**Spark Worker** — executes the actual computation on its slice of the data.

When you submit a job:
1. Your script creates a `SparkSession` and describes a transformation (filter, group by, join, etc.)
2. Spark builds an **execution plan** — it figures out the most efficient way to run it
3. The Master breaks the plan into **tasks** and sends them to Workers
4. Each Worker reads its portion of the data directly from HDFS DataNodes and processes it
5. Results are collected and written back to HDFS

```
  spark-submit job.py
        │
        ▼
  ┌─────────────┐
  │ Spark Master│  ← plans and coordinates
  └──────┬──────┘
         │ assigns tasks
  ┌──────▼──────┐    ┌─────────────┐
  │Spark Worker │    │Spark Worker │  ← run in parallel
  │  reads from │    │  reads from │
  │  DataNode A │    │  DataNode B │
  └─────────────┘    └─────────────┘
```

The key insight: Spark moves the **computation to the data**, not the data to the computation. Each Worker reads the blocks that are physically closest to it. This is why Spark is commonly run on the same machines as HDFS DataNodes in production.

---

## Lazy evaluation — Spark does not run when you tell it to

This is the thing that confuses most people coming from pandas.

```python
df = spark.read.csv("hdfs://namenode:9000/input/iris.csv")
filtered = df.filter(df["species"] == "Iris-setosa")  # nothing runs here
filtered.write.csv("hdfs://namenode:9000/output/")    # THIS triggers execution
```

The first two lines just build a plan. Spark waits until you call an **action** (`.write`, `.collect`, `.count`, `.show`) and then executes the whole plan at once. This lets Spark optimise across all the steps — it might reorder operations, push filters earlier, skip reading data it knows it won't need.

This is called **lazy evaluation**. It is a feature, not a quirk.

---

## Batch vs Streaming — a design decision, not a technical detail

The project requires two modes. Understanding why they are different is more important than knowing how to implement either.

**Batch processing**: you have a dataset that is complete. Run once, produce output, done. The entire dataset exists before processing starts.

**Stream processing**: data arrives continuously or at unpredictable intervals. You cannot wait for the dataset to be "complete" because it never is. Processing must happen automatically as data arrives.

In this project:
- **Batch** — extract the full CSV, then transform it. The file exists before Spark starts.
- **Stream** — Spark watches `Input_dir` permanently. Whenever extract drops a new file there, Spark detects it and processes it automatically. Extract and transform are fully decoupled in time.

The stream mode models how production pipelines actually work. Sensors, APIs, and data feeds do not wait for you. The processing system must react to arrivals, not be triggered manually.

Spark Structured Streaming uses the same dataframe API as batch. You read from a streaming source instead of a static file, and use `.writeStream` instead of `.write`. The transformation logic in the middle is identical.

```python
# Batch
df = spark.read.csv(input_path)
df.filter(...).write.csv(output_path)

# Stream — same filter, different source and sink
df = spark.readStream.csv(input_path)
df.filter(...).writeStream.start(output_path)
```

---

## Why spark-submit, not `python script.py`

You cannot run PySpark jobs with the regular Python interpreter on your machine. The job needs to run **inside the Spark cluster** so the Master can distribute it to Workers.

`spark-submit` is the CLI that packages your script and sends it to the Master:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/transform_batch.py
```

In this project, `submit_jobs.py` wraps this command using Python's `subprocess`. The reason for using subprocess with `shell=False` (list form) is covered in [security.md](security.md).

---

## Key terms

| Term | Meaning |
|---|---|
| SparkSession | The entry point to Spark in your code. One per application. |
| DataFrame | Spark's distributed table abstraction. Like a pandas DataFrame but split across Workers. |
| Transformation | An operation that produces a new DataFrame (filter, select, join). Lazy — does not run yet. |
| Action | An operation that triggers execution and produces a result (write, count, collect). |
| Task | A unit of work sent to a Worker. One task per data partition. |
| Partition | A chunk of a DataFrame held by one Worker. More partitions = more parallelism. |
| DAG | Directed Acyclic Graph — the execution plan Spark builds from your transformations. |
| Structured Streaming | Spark's streaming engine. Treats a stream as an unbounded table and applies batch-like queries to it continuously. |
