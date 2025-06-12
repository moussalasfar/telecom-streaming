from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, from_json, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import psycopg2
import traceback
import sys
from pyspark.sql.functions import to_timestamp

# PostgreSQL configuration
pg_host = "postgres"
pg_port = "5432"
pg_db = "telecomdb"
pg_user = "postgres"
pg_password = "postgres"

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
jdbc_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Step 1: Create the rated table if not exists
def create_rated_table():
    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rated_usage_records (
                record_type TEXT,
                timestamp TIMESTAMP,
                user_id TEXT,
                data_volume_mb FLOAT,
                session_duration_sec FLOAT,
                caller_id TEXT,
                callee_id TEXT,
                duration_sec FLOAT,
                sender_id TEXT,
                receiver_id TEXT,
                cell_id TEXT,
                technology TEXT,
                status TEXT,
                cost FLOAT
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table rated_usage_records prête.")
    except Exception as e:
        print("❌ Erreur lors de la création de la table :", e)
        traceback.print_exc()
        sys.exit(1)

create_rated_table()

# Step 2: Start Spark session
spark = SparkSession.builder \
    .appName("TelecomRatingJob") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Step 3: Define schema and read from Kafka
schema = StructType([
    StructField("record_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("caller_id", StringType()),
    StructField("callee_id", StringType()),
    StructField("sender_id", StringType()),
    StructField("receiver_id", StringType()),
    StructField("duration_sec", StringType()),  # Cast later
    StructField("data_volume_mb", StringType()),  # Cast later
    StructField("session_duration_sec", StringType()),  # Cast later
    StructField("cell_id", StringType()),
    StructField("technology", StringType())
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "normalized_telecom_data") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Step 4: Secure casting

df_casted = df_parsed \
    .withColumn("duration_sec", col("duration_sec").cast("double")) \
    .withColumn("data_volume_mb", col("data_volume_mb").cast("double")) \
    .withColumn("session_duration_sec", col("session_duration_sec").cast("double")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"))  # <-- ICI


# Step 5: Rating logic
df_rated = df_casted \
    .withColumn("status", when(col("record_type").isNull(), lit("rejected"))
        .when((col("record_type") == "data") & col("data_volume_mb").isNull(), lit("error"))
        .when((col("record_type") == "voice") & col("duration_sec").isNull(), lit("error"))
        .when((col("record_type") == "sms") & col("sender_id").isNull(), lit("error"))
        .otherwise(lit("rated"))
    ) \
    .withColumn("cost", when((col("record_type") == "data") & (col("status") == "rated"),
                             when(col("data_volume_mb") <= 100, col("data_volume_mb") * 5.0)
                             .otherwise((100 * 5.0) + ((col("data_volume_mb") - 100) * 2.0)))
        .when((col("record_type") == "voice") & (col("status") == "rated"),
              spark_round((col("duration_sec") / 60.0) + 0.5) * 1.0)
        .when((col("record_type") == "sms") & (col("status") == "rated"), lit(0.5))
        .otherwise(lit(0.0))
    )

# Step 6: Save to PostgreSQL
def save_to_postgresql(batch_df, batch_id):
    try:
        print(f"💾 Batch {batch_id} : {batch_df.count()} enregistrements")
        batch_df.write.jdbc(
            url=jdbc_url,
            table="rated_usage_records",
            mode="append",
            properties=jdbc_properties
        )
        print(f"✅ Batch {batch_id} enregistré avec succès")
    except Exception as e:
        print(f"❌ Erreur dans le batch {batch_id} :", e)
        traceback.print_exc()

query = df_rated.writeStream \
    .foreachBatch(save_to_postgresql) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints-rating") \
    .start()

query.awaitTermination()
