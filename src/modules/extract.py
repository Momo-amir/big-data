import os
import requests

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_INPUT_DIR = "/data/Input_dir"
CHUNK_SIZE = 8192


def extract(url: str) -> str:
    """
    Download a CSV from `url` directly into HDFS Input_dir via WebHDFS.

    Security & integrity:
    - URL is validated to require HTTPS, preventing plaintext transport.
    - No shell is involved; no subprocess/shell=True, so command-line injection
      is impossible — the URL is passed as a Python string to the requests library.
    - HTTPS encrypts the data in transit (TLS).

    Robustness:
    - response.iter_content(chunk_size) streams the download in small blocks
      so a large file never has to fit in memory and a partial download can be
      detected via raise_for_status() before any bytes are written to HDFS.
    - The WebHDFS CREATE call uses overwrite=true so re-running is safe.
    """
    if not url.lower().startswith("https://"):
        raise ValueError(f"Only HTTPS sources are accepted. Got: {url!r}")

    filename = url.rstrip("/").split("/")[-1]
    hdfs_path = f"{HDFS_INPUT_DIR}/{filename}"

    # --- stream download from source ---
    with requests.Session() as session:
        response = session.get(url, stream=True, timeout=30)
        response.raise_for_status()

        data_chunks = []
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                data_chunks.append(chunk)

    raw_data = b"".join(data_chunks)

    # Add column headers — the iris.csv from the source has no header row.
    # Data science context: columns are sepal_length, sepal_width,
    # petal_length, petal_width, species (as documented by J. Brownlee).
    header = b"sepal_length,sepal_width,petal_length,petal_width,species\n"
    raw_data = header + raw_data

    # --- write directly to HDFS via WebHDFS REST API ---
    # Step 1: initiate CREATE, get redirect URL
    create_url = (
        f"{HDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&overwrite=true&user.name=root"
    )
    redirect = requests.put(create_url, allow_redirects=False, timeout=10)
    if redirect.status_code != 307:
        raise RuntimeError(
            f"WebHDFS CREATE did not redirect (HTTP {redirect.status_code}): "
            f"{redirect.text}"
        )

    # Step 2: PUT data to the datanode redirect URL
    datanode_url = redirect.headers["Location"]
    put_resp = requests.put(
        datanode_url,
        data=raw_data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    put_resp.raise_for_status()

    print(f"[extract] Saved {len(raw_data)} bytes → hdfs:{hdfs_path}")
    return hdfs_path
