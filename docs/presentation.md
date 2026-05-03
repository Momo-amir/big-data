# Big Data Pipeline — Præsentation

## Opbygning: 3 akter + live demo

---

## Akt 1 — Platformen (infrastruktur, ~5 min)

Vis `docker-compose.yml` og forklar de 4 distribuerede systemer med én sætning hver:

| System | Rolle i pipeline | Port |
|--------|-----------------|------|
| **Hadoop HDFS** | Data lake — rå og transformeret data | `9870` (UI) |
| **Apache Spark** | Distribueret compute engine | `8080` (UI) |
| **Apache Hive** | Data warehouse — SQL over HDFS via MySQL metastore | `10000` (JDBC) |
| **Apache Kafka** | Event streaming — live producer/consumer | `9092` |

Nøglepunkt: det er én `docker compose up -d` der starter det hele — det simulerer et produktions-cluster lokalt.

---

## Akt 2 — De 3 dataflows (kode + terminal, ~15 min)

### Flow 1: Struktureret data (Batch ETL)

```
HTTPS → HDFS Input_dir → Spark filter → Hive tabel + HDFS CSV (krypteret) → PNG diagrammer → Kafka
```

Vis [`src/main.py`](../src/main.py) — den rene ETL-sekvens på 15 linjer. Gå derefter ét niveau ned:

**extract.py** — HTTPS-validering + chunked streaming + WebHDFS PUT direkte til HDFS:
```python
# Kun HTTPS accepteres — ingen command-line injection (ingen subprocess/shell=True)
if not url.lower().startswith("https://"):
    raise ValueError(...)

# Streames i blokke på 8192 bytes — store filer passer aldrig i RAM
for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
    data_chunks.append(chunk)

# Skrives direkte til HDFS via WebHDFS REST — rører aldrig det lokale filsystem
requests.put(datanode_url, data=raw_data, ...)
```

**transform_batch.py** — Spark læser fra HDFS og filtrerer:
```python
spark = SparkSession.builder
    .master("spark://spark-master:7077")   # kører på det installerede Spark cluster
    .enableHiveSupport()                   # Spark kan skrive direkte til Hive
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/Input_dir/iris.csv", header=True)
filtered = df.filter(df["species"] == "Iris-setosa")   # 50 rækker ud af 150
```

**load.py** — krypterer hvert felt og gemmer på HDFS + Hive:
```python
# Hvert celle-felt krypteres med AES-256-GCM inden det gemmes
writer.writerow([encrypt(str(v)) for v in row])

# Hive: databasen oprettes automatisk hvis den ikke eksisterer
spark.sql("CREATE DATABASE IF NOT EXISTS irisdb")
enc_df.write.mode("overwrite").format("hive").saveAsTable("irisdb.iris_setosa")
```

**Terminal-kommandoer:**
```bash
bash scripts/start.sh batch

# Bekræft HDFS output
docker exec namenode hdfs dfs -ls /data/Output_dir

# Se krypteret CSV
docker exec namenode hdfs dfs -cat /data/Output_dir/transformed_iris.csv | head -3

# Bekræft krypteret data i Hive
docker exec hive-server beeline -n hive \
  -u "jdbc:hive2://hive-server:10000" \
  -e "SELECT * FROM irisdb.iris_setosa LIMIT 3;"
```

---

### Flow 2: Semi-/ustruktureret data (Real-time / Structured Streaming)

```
HDFS Input_dir (watch) → Spark Structured Streaming micro-batch → Load + Viz + Kafka
```

Vis [`src/modules/transform_stream.py`](../src/modules/transform_stream.py) — fokus på de tre linjer der forklarer alt:

```python
# Spark poller HDFS automatisk — ingen ekstra daemon eller cron-job nødvendigt
spark.readStream
    .schema(IRIS_SCHEMA)             # fast schema, ingen re-scan af gamle filer
    .option("maxFilesPerTrigger", 1) # én fil ad gangen
    .csv("hdfs://namenode:9000/data/Input_dir")
```

`foreachBatch` callbacket kalder de **samme** Load/Viz/Kafka-funktioner som batch — én kodebase, to modes:

```python
def _process_batch(batch_df, batch_id, spark):
    filtered = batch_df.filter(col("species") == "Iris-setosa")
    save_csv(filtered, "iris.csv")       # samme load.py som batch
    save_to_hive(filtered, spark)        # samme load.py som batch
    scatter_plot(fetch_iris_data(spark)) # samme visualization.py
    stream_to_kafka(spark)               # samme kafka_producer.py
```

**Terminal-kommandoer:**
```bash
bash scripts/start.sh stream

# Terminal 1: følg stream-watcher
docker exec etl-runner tail -f /tmp/stream.log

# Terminal 2: følg Kafka consumer
docker exec etl-runner tail -f /tmp/consumer.log

# Trigger extract manuelt — stream reagerer automatisk
docker exec etl-runner python extract_only.py
```

---

### Flow 3: Streaming data (Kafka producer → consumer)

```
Hive tabel (krypteret) → decrypt → KafkaProducer → topic iris-setosa → KafkaConsumer → konsol
```

**kafka_producer.py** — læser fra Hive, dekrypterer og sender til Kafka:
```python
df = spark.sql("SELECT * FROM irisdb.iris_setosa")

producer = KafkaProducer(bootstrap_servers="kafka:9092", ...)

for row in df.collect():
    record = {col: decrypt(str(row[i])) for i, col in enumerate(columns)}
    producer.send("iris-setosa", value=record)  # plaintext JSON til consumer
```

**consumer.py** — selvstændig app der subscriber til topiket:
```python
consumer = KafkaConsumer("iris-setosa", bootstrap_servers="kafka:9092",
                          auto_offset_reset="earliest", ...)

for message in consumer:
    print(message.value)  # {"sepal_length": "5.1", "species": "Iris-setosa", ...}
```

Nøglepunkt: **decoupling** — producer og consumer kender ikke hinanden. Kafka er bufferen. Consumer kan køre hos "kunden" helt selvstændigt.

---

## Akt 3 — Sikkerhed på tværs af alle flows (~3 min)

Vis [`src/modules/security.py`](../src/modules/security.py):

```python
# AES-256-GCM: AEAD — giver både fortrolighed OG integritet
# En 16-byte authentication tag opdager hvis ciphertext er blevet manipuleret
# Tilfældig 12-byte nonce per kald → samme plaintext giver forskelligt ciphertext

def encrypt(plaintext: str) -> str:
    nonce = os.urandom(12)
    ct_with_tag = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct_with_tag).decode()

# Wire format: base64(nonce[12] || ciphertext || tag[16])
```

**Valg: GCM frem for CBC** — GCM er valgt fordi det udover kryptering også giver integritetsbeskyttelse (AEAD). CBC krypterer men opdager ikke manipulation af ciphertext.

Data er krypteret **på skrivetidspunktet** i hele pipeline:
- CSV på HDFS → krypteret
- Rækker i Hive → krypteret
- Kun dekrypteret når Kafka streamer til consumer, eller viz læser fra Hive

---

## Full demo — kommandoer i rækkefølge

```bash
# 1. Start hele clusteret og kør alle pipelines
bash scripts/start.sh all

# 2. Bekræft HDFS
docker exec namenode hdfs dfs -ls /data/Input_dir
docker exec namenode hdfs dfs -ls /data/Output_dir
docker exec namenode hdfs dfs -cat /data/Output_dir/transformed_iris.csv | head -3

# 3. Bekræft Hive (krypteret data)
docker exec hive-server beeline -n hive \
  -u "jdbc:hive2://hive-server:10000" \
  -e "SELECT * FROM irisdb.iris_setosa LIMIT 3;"

# 4. Vis genererede diagrammer på HDFS
docker exec namenode hdfs dfs -ls /data/Output_dir

# 5. Vis Kafka consumer modtager plaintext
docker exec etl-runner tail -20 /tmp/consumer.log

# 6. Demonstrer real-time reaktion: drop en ny fil og se stream-watcher reagere
docker exec etl-runner python extract_only.py
docker exec etl-runner tail -f /tmp/stream.log
```

---

## Den korte version

Én kommando — `bash scripts/start.sh` — starter 6 distribuerede services og kører data hele vejen fra en HTTPS-kilde til:
- Krypterede filer på **HDFS** (data lake)
- SQL-queries i **Hive** (data warehouse)
- PNG-diagrammer på **HDFS** (visualisering)
- Live JSON-beskeder i **Kafka** (event streaming)
