from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# Define schema
schema = StructType([
    StructField("record_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("caller_id", StringType()),
    StructField("callee_id", StringType()),
    StructField("sender_id", StringType()),
    StructField("receiver_id", StringType()),
    StructField("duration_sec", DoubleType()),
    StructField("data_volume_mb", DoubleType()),
    StructField("session_duration_sec", DoubleType()),
    StructField("cell_id", StringType()),
    StructField("technology", StringType())
])

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

# Read from raw_telecom_data topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_telecom_data") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

# Normalize/Filter valid records
valid_df = json_df.filter(
    ((col("record_type") == "voice") & (col("duration_sec") > 0)) |
    ((col("record_type") == "data") & (col("data_volume_mb") > 0)) |
    (col("record_type") == "sms")
)

# Write the valid records to normalized_telecom_data topic
query = valid_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "normalized_telecom_data") \
    .option("checkpointLocation", "/tmp/checkpoints-mediation") \
    .outputMode("append") \
    .start()

query.awaitTermination()
