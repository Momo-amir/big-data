import logging
from hdfs import InsecureClient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8192  # 8 KB per write chunk


def connect(hdfs_url: str) -> InsecureClient:
    client = InsecureClient(hdfs_url, user="root")
    logger.info("Connected to HDFS at %s", hdfs_url)
    return client


def ensure_dir(client: InsecureClient, hdfs_dir: str) -> None:
    client.makedirs(hdfs_dir, permission="755")
    logger.debug("Ensured HDFS directory: %s", hdfs_dir)


def write_stream(client: InsecureClient, hdfs_path: str, response_iter) -> int:
    """
    Writes a streaming HTTP response to HDFS chunk by chunk.
    response_iter must yield bytes (e.g. response.iter_content(chunk_size=CHUNK_SIZE)).
    Returns total bytes written.
    """
    total = 0
    with client.write(hdfs_path, overwrite=True, buffersize=CHUNK_SIZE) as writer:
        for chunk in response_iter:
            if chunk:
                writer.write(chunk)
                total += len(chunk)
    logger.info("Wrote %d bytes to HDFS path: %s", total, hdfs_path)
    return total
