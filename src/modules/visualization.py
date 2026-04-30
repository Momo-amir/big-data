"""
Visualization module — reads decrypted Iris-setosa data from Hive and
generates scatter plot, histogram, and boxplot diagrams saved to HDFS.
"""

import io
import os
import requests
import matplotlib
matplotlib.use("Agg")  # non-interactive backend, safe inside a container
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

from modules.security import decrypt

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_OUTPUT_DIR = "/data/Output_dir"
HIVE_DB = os.getenv("HIVE_DB", "irisdb")
HIVE_TABLE = os.getenv("HIVE_TABLE", "iris_setosa")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hdfs_write_bytes(hdfs_path: str, data: bytes) -> None:
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
    put_resp = requests.put(
        redirect.headers["Location"],
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    put_resp.raise_for_status()


def _save_fig(fig: plt.Figure, filename: str) -> None:
    """Save a matplotlib figure as PNG directly to HDFS Output_dir."""
    hdfs_path = f"{HDFS_OUTPUT_DIR}/{filename}"
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    _hdfs_write_bytes(hdfs_path, buf.read())
    plt.close(fig)
    print(f"[viz] Diagram saved → hdfs:{hdfs_path}")


def fetch_iris_data(spark: SparkSession) -> dict:
    """
    Read the encrypted Hive table, decrypt every value, and return a dict
    of column-name → list-of-floats ready for matplotlib.
    """
    spark.sql(f"USE {HIVE_DB}")
    df = spark.sql(f"SELECT sepal_length, sepal_width, petal_length, petal_width FROM {HIVE_TABLE}")
    rows = df.collect()
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    return {col: [float(decrypt(row[i])) for row in rows] for i, col in enumerate(columns)}


# ---------------------------------------------------------------------------
# Public API — each method accepts a pre-fetched data dict
# ---------------------------------------------------------------------------

def scatter_plot(data: dict) -> None:
    """
    Scatter plot: sepal_length (x) vs petal_length (y).
    """
    fig, ax = plt.subplots()
    ax.scatter(data["sepal_length"], data["petal_length"], color="steelblue", alpha=0.7)
    ax.set_xlabel("Sepal Length (cm)")
    ax.set_ylabel("Petal Length (cm)")
    ax.set_title("Iris-setosa: Sepal Length vs Petal Length")
    ax.grid(True)
    _save_fig(fig, "scatter_plot.png")


def histogram(data: dict) -> None:
    """
    Histogram of petal_width with 10 bins.
    """
    fig, ax = plt.subplots()
    ax.hist(data["petal_width"], bins=10, color="coral", edgecolor="black")
    ax.set_xlabel("Petal Width (cm)")
    ax.set_ylabel("Frequency")
    ax.set_title("Iris-setosa: Distribution of Petal Width")
    ax.grid(True, axis="y")
    _save_fig(fig, "histogram.png")


def boxplot(data: dict) -> None:
    """
    2×2 layout with boxplots for sepal_length, sepal_width,
    petal_length, and petal_width.
    """
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    fig.suptitle("Iris-setosa — Measurement Boxplots", fontsize=14)

    fields = [
        ("sepal_length", "Sepal Length (cm)"),
        ("sepal_width",  "Sepal Width (cm)"),
        ("petal_length", "Petal Length (cm)"),
        ("petal_width",  "Petal Width (cm)"),
    ]

    for ax, (col, label) in zip(axes.flat, fields):
        ax.boxplot(data[col], vert=True, patch_artist=True,
                   boxprops=dict(facecolor="lightblue"))
        ax.set_title(label)
        ax.set_ylabel("cm")
        ax.grid(True, axis="y")

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    _save_fig(fig, "boxplot.png")
