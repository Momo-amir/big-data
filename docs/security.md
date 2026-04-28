# Security — Why These Constraints Exist

The assignment specifies several security requirements that might look like bureaucratic rules. They are not. Each one addresses a concrete attack or failure mode. This document explains the actual threat behind each requirement so you can implement them correctly rather than just satisfying the spec by surface appearance.

---

## HTTPS — protecting data in transit

When you fetch data over plain HTTP, every network device between your machine and the server can read the content. On a corporate or university network this includes routers, proxies, and potentially other machines on the same subnet.

More critically: plain HTTP cannot verify that you are talking to the server you think you are. A man-in-the-middle attack replaces the real server's response with crafted data — your pipeline processes it as if it were legitimate. For scientific data, this means corrupted research results with no indication anything went wrong.

HTTPS solves both problems:
- **Encryption** — the payload is encrypted in transit; a network observer sees noise
- **Authentication** — the server presents a certificate signed by a trusted authority; your client verifies it before accepting any data

In Python `requests`, HTTPS certificate verification is on by default. Do not disable it with `verify=False`. That defeats the authentication half of HTTPS and leaves you vulnerable to exactly the attack it prevents.

Your `validation.py` module should reject any URL that does not start with `https://` before the request is ever made. This prevents a misconfigured URL or injected input from silently downgrading to an insecure connection.

```python
# Wrong — will silently accept http://
response = requests.get(url)

# Right — validate before the request
if not url.startswith("https://"):
    raise ValueError(f"Insecure URL rejected: {url}")
response = requests.get(url)
```

---

## Command injection — why subprocess needs shell=False

This project uses `subprocess` to call `spark-submit`. There are two ways to do it:

```python
# String form — dangerous
subprocess.run(f"spark-submit --master {master_url} {job_path}", shell=True)

# List form — safe
subprocess.run(["spark-submit", "--master", master_url, job_path], shell=False)
```

With `shell=True`, the string is passed to the OS shell (`/bin/sh`). The shell interprets special characters. If `master_url` or `job_path` contains user-supplied input and an attacker can influence that input, they can inject shell commands:

```
master_url = "spark://host:7077; rm -rf /"
```

With `shell=True` the shell executes both `spark-submit` and `rm -rf /` as separate commands.

With `shell=False` and a list, there is no shell involved. Each element in the list is passed as a literal argument to the program. Special characters are not interpreted. The injection above becomes a literal argument string that `spark-submit` rejects as an invalid master URL.

**The rule**: whenever you construct a command from any variable — even one you control — use the list form and `shell=False`. The cost is zero. The protection is real.

---

## Chunked streaming — why not download the whole file first?

The naive approach to downloading a file:

```python
response = requests.get(url)
data = response.content  # entire file loaded into RAM
```

This works for small files. For a 10 GB CSV, it allocates 10 GB of RAM and waits for the entire download before doing anything with the data. If the connection drops at 9.9 GB, you start over.

The robust approach streams in chunks:

```python
with requests.get(url, stream=True) as response:
    for chunk in response.iter_content(chunk_size=8192):
        hdfs_writer.write(chunk)
```

This has three properties the naive approach lacks:

**Memory bounded** — at any moment, only one chunk (8 KB in this example) is in RAM, regardless of file size. A 100 GB file uses the same memory as a 1 MB file.

**Resilient to interruption** — if the connection drops mid-download, you lose only the current chunk. With a retry mechanism, you can resume. With the naive approach, you lose everything.

**Pipelined** — chunks can be written to HDFS as they arrive. Download and write happen concurrently rather than sequentially. The pipeline is faster.

This is why the extract module writes to HDFS chunk by chunk rather than downloading to disk first and uploading second.

---

## Putting it together — the threat model

These three controls address different points in the data flow:

```
[Attacker controls URL input]
          │
          ▼
    validation.py ──── rejects non-HTTPS URLs (injection, downgrade)
          │
          ▼
    requests (HTTPS) ── encrypted, authenticated transport
          │
          ▼
    chunked write ───── memory-safe, resumable
          │
          ▼
         HDFS
          │
          ▼
    submit_jobs.py ──── shell=False (command injection on spark-submit)
          │
          ▼
        Spark
```

None of these is particularly hard to implement. The point is knowing which threat each one addresses, so when you are tempted to skip one ("nobody will actually inject a URL here"), you understand what you are accepting as a risk.
