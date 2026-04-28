# Hadoop & HDFS — Why Distributed Storage?

## The problem it solves

Imagine you have a CSV file that is 2 TB. A normal server has maybe 500 GB of disk. You cannot store it. You buy a bigger server — now you have 4 TB, problem solved for now. But next year the file is 8 TB. You buy an even bigger server. This approach has a name: **vertical scaling**, and it has a hard ceiling. At some point, no single machine is big enough or fast enough.

HDFS (Hadoop Distributed File System) takes a different approach: **horizontal scaling**. Instead of one big machine, you use many ordinary machines. The file is split into blocks and spread across all of them. When you need more capacity, you add another machine. There is no ceiling.

This is the core trade-off of distributed systems: **complexity in exchange for unlimited scale**.

---

## How HDFS actually works

HDFS has two kinds of nodes:

**NameNode** — the brain. It does not store any actual file data. It stores the *map* of where everything is: which blocks belong to which file, and which DataNodes hold which blocks. There is one NameNode per cluster.

**DataNode** — the muscle. These store the actual blocks of data. A cluster can have hundreds of them.

When you write a file:
1. The client asks the NameNode: "I want to write `iris.csv`"
2. The NameNode replies: "Split it into blocks, send block 1 to DataNode-A, block 2 to DataNode-B..."
3. The client streams directly to the DataNodes — the NameNode is not in that data path at all
4. Each block is also replicated to 2 other DataNodes automatically (default replication factor: 3)

When you read a file:
1. The client asks the NameNode: "Where is `iris.csv`?"
2. The NameNode returns a block map
3. The client reads each block directly from the nearest DataNode

```
              ┌─────────────┐
              │  NameNode   │  ← knows WHERE everything is
              │  (metadata) │
              └──────┬──────┘
                     │ "block 1 → DataNode-A, block 2 → DataNode-B"
              ┌──────▼──────┐
   Client ───►│   DataNode  │  ← stores actual data
              │      A      │
              └─────────────┘
              ┌─────────────┐
   Client ───►│   DataNode  │
              │      B      │
              └─────────────┘
```

The replication means if DataNode-A dies, the data is still on two other nodes. The NameNode detects the failure and re-replicates the lost blocks automatically.

---

## Why write directly to HDFS — not local disk first?

This project requires that extract writes straight to HDFS without touching local disk. This is not arbitrary — it is the correct architecture for a pipeline that needs to scale.

If you write to local disk first and then copy to HDFS, you have two problems:
- **You need enough local disk** to hold the file. With very large files, you may not.
- **You move the data twice** — download → local disk → HDFS. That is double the I/O and double the time.

Streaming directly to HDFS means: the download comes in, chunks go straight out to DataNodes. Local disk is never involved. The pipeline can handle a file of any size on a machine with almost no disk space.

---

## Single-node cluster in development

In production, HDFS runs across tens or hundreds of machines. In this project, the NameNode and DataNode both run as Docker containers on your laptop. This is called a **single-node cluster** or pseudo-distributed mode.

The reason this works without changing any application code: your Python code talks to HDFS via the NameNode's HTTP API at a URL (`http://namenode:9870`). In production, you would change that URL to point at the real cluster. The code is identical.

This is a fundamental property of distributed systems design: **location transparency**. The application should not care whether the cluster has 1 node or 1000.

---

## Key terms

| Term | Meaning |
|---|---|
| Block | A fixed-size chunk of a file (default 128 MB). HDFS splits files into blocks. |
| NameNode | Stores metadata (the file→block map). One per cluster. |
| DataNode | Stores actual block data. Many per cluster. |
| Replication | Each block is copied to N DataNodes (default 3) for fault tolerance. |
| Safe mode | State the NameNode enters on startup while it waits for DataNodes to report in. No writes allowed until it exits safe mode. |
| WebHDFS | The HTTP REST API for HDFS. Used by the Python `hdfs` library to read/write files without installing Hadoop locally. |
