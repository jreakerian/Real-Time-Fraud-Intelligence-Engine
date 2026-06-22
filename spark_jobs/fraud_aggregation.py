from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg
import os
import time

print("Starting Spark Aggregation Job...")

spark_remote = os.getenv("SPARK_REMOTE", "sc://localhost:15002")

spark = None
while not spark:
    try:
        temp_spark = SparkSession.builder \
            .appName("FraudAnalytics_Connect") \
            .remote(spark_remote) \
            .getOrCreate()
        temp_spark.sql("SELECT 1").collect()
        spark = temp_spark
    except Exception:
        print(f"Waiting for Spark Connect at {spark_remote}...")
        time.sleep(5)
    
input_path = "file:///opt/shared_deps/sample_data/transactions.csv"
analytics_path = "file:///opt/shared_deps/data_lake/analytics/fraud_summary"

print(f"Reading processed data from: {input_path}")

df = spark.read.option("header", True).csv(input_path)

print("Calculating aggregations...")
agg = df.groupby("location").agg(
    count("*").alias("total_transactions"),
    avg("amount").alias("avg_amount")
)

print(f"Writing analytics to: {analytics_path}")
agg.write.mode("overwrite").parquet(analytics_path)

print("\nAggregation complete. Stopping Spark Session.")
agg.show()

spark.stop()