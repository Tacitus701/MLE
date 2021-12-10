from pyspark.sql import SparkSession
import findspark
import os

findspark.init()

spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka") \
    .master("local[*]") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "latest") \
    .load()

df.printSchema()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

print("Finished")
