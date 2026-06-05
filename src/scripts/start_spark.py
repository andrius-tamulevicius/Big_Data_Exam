from pyspark.sql import SparkSession


def start_spark() -> SparkSession:
    return SparkSession.builder.appName("collision-detection").getOrCreate()
