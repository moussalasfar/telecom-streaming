import sys
import traceback
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum as spark_sum, date_format, current_timestamp, lit
)

# PostgreSQL Configuration
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

# Step 1: Create billing_summary table if it doesn't exist
def create_billing_summary_table():
    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_summary (
                customer_id TEXT,
                billing_period TEXT,
                total_cost FLOAT,
                generated_at TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table billing_summary créée ou déjà existante.")
    except Exception as e:
        print("❌ Erreur lors de la création de billing_summary :", e)
        traceback.print_exc()
        sys.exit(1)

create_billing_summary_table()

# Step 2: Start Spark session
spark = SparkSession.builder \
    .appName("BillingEngine") \
    .getOrCreate()

# Step 3: Load rated usage records
rated_df = spark.read.jdbc(
    url=jdbc_url,
    table="rated_usage_records",
    properties=jdbc_properties
)

# Step 4: Enrich records
rated_df = rated_df.withColumn("billing_period", date_format("timestamp", "yyyy-MM"))

# Only keep successfully rated records
rated_df = rated_df.filter(col("status") == "rated")

# Unified customer_id: user_id for data, caller_id for voice, sender_id for sms
rated_df = rated_df.withColumn(
    "customer_id",
    when(col("record_type") == "data", col("user_id"))
    .when(col("record_type") == "voice", col("caller_id"))
    .when(col("record_type") == "sms", col("sender_id"))
)

# Step 5: Aggregate by customer_id and billing_period, sum cost
billing_df = rated_df.groupBy("customer_id", "billing_period") \
    .agg(
        spark_sum("cost").alias("total_cost")
    ) \
    .withColumn("generated_at", current_timestamp())

# Step 6: Check for any missing critical fields before saving
missing_data_check = billing_df.filter(col("customer_id").isNull() | col("billing_period").isNull())
if missing_data_check.count() > 0:
    print("❌ Found records with missing critical data")
    missing_data_check.show()
else:
    print("✅ All data is valid.")

    # Step 7: Upsert result to PostgreSQL (delete old, insert new for each customer_id/billing_period)
    import pandas as pd
    new_rows = billing_df.select(
        "customer_id", "billing_period", "total_cost", "generated_at"
    ).toPandas()
    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password
        )
        cur = conn.cursor()
        for _, row in new_rows.iterrows():
            cur.execute(
                "DELETE FROM billing_summary WHERE customer_id=%s AND billing_period=%s",
                (row['customer_id'], row['billing_period'])
            )
        conn.commit()
        cur.close()
        conn.close()
        print(f"🗑️ Old rows deleted for upsert.")
    except Exception as e:
        print(f"❌ Error deleting old rows for upsert: {e}")
        traceback.print_exc()

    # Now insert the new rows
    billing_df.select(
        "customer_id", "billing_period", "total_cost", "generated_at"
    ).write.jdbc(
        url=jdbc_url,
        table="billing_summary",
        mode="append",
        properties=jdbc_properties
    )

    print("✅ Billing process terminé avec succès.")
