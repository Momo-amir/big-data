import matplotlib.pyplot as plt
from modules.database import get_connection
from modules.security import decrypt


def fetch_iris_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sepal_length, sepal_width, petal_length, petal_width FROM iris_setosa")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    data = {col: [float(decrypt(row[i])) for row in rows] for i, col in enumerate(columns)}
    return data


def scatter_plot(x, y, x_label, y_label, title=None, output_path="src/_output/plot.png"):
    plt.figure()
    plt.scatter(x, y)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title or f"{x_label} vs {y_label}")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Graph saved to {output_path}")


def histogram(x, bins=10, x_label="Value", y_label="Frequency", title="Histogram", output_path="src/_output/histogram.png"):
    plt.figure()
    plt.hist(x, bins=bins)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Graph saved to {output_path}")
    
def boxplot(sepal_length, sepal_width, petal_length, petal_width, title="Iris Setosa — Boxplots", output_path="src/_output/boxplot.png"):
    labels = ["Sepal Length", "Sepal Width", "Petal Length", "Petal Width"]
    values = [sepal_length, sepal_width, petal_length, petal_width]

    plt.figure(figsize=(10, 6))
    plt.boxplot(values, labels=labels)
    plt.title(title)
    plt.ylabel("cm")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Graph saved to {output_path}")