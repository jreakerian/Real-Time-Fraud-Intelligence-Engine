from pyspark.sql import SparkSession
import os
from pathlib import Path
import time

print("Starting Spark Batch Job...")

spark_remote = os.getenv("SPARK_REMOTE", "sc://localhost:15002")

spark = SparkSession.builder \
    .appName("FraudBatch") \
    .remote(spark_remote) \
    .getOrCreate()
spark = None
while not spark:
    try:
        temp_spark = SparkSession.builder \
            .appName("FraudBatch") \
            .remote(spark_remote) \
            .getOrCreate()
        temp_spark.sql("SELECT 1").collect()
        spark = temp_spark
    except Exception:
        print(f"Waiting for Spark Connect at {spark_remote}...")
        time.sleep(5)
    
# Function to convert a local file path to a file:// URI
def local_path_to_uri(path):
    # This automatically handles Windows drive letters and forward slashes correctly
    return Path(path).absolute().as_uri()

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
input_raw_path = os.path.join(project_root, "sample_data", "transactions.csv")
output_raw_path = os.path.join(project_root, "data_lake", "processed", "fraud_results")

#input_path_uri = local_path_to_uri(input_raw_path)
#output_path_uri = local_path_to_uri(output_raw_path)

input_path_uri = "file:///opt/shared_deps/sample_data/transactions.csv"
output_path_uri = "file:///opt/shared_deps/data_lake/processed/fraud_results"

print("Input:", input_path_uri)
print("Output:", output_path_uri)

df = spark.read.option("header", True).csv(input_path_uri)

print("Data Loaded")
df.show(5)

print("Row count:", df.count())
print("Writing to:", output_path_uri)

df.write.mode("overwrite").parquet(output_path_uri)

print("Write complete. Stopping Spark Session.")

spark.stop()