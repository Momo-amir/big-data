from pyspark.sql import SparkSession


def filter_by_value(csv_path, column, value):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(csv_path, header=False).toDF('sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species')
    return df.filter(df[column] == value)
