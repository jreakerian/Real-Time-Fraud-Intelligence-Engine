import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

import grpc # import grpc to RpcError
import logging
import time

logging.basicConfig(level=logging.INFO)

spark_remote = os.getenv("SPARK_REMOTE", "sc://localhost:15002")

spark = None
while not spark:
    try:
        spark = SparkSession.builder \
            .remote(spark_remote) \
            .appName("FraudDetection") \
            .getOrCreate()
        logging.info("Spark Session created successfully.")
    except Exception as e:
        logging.error(f"Waiting for Spark Connect server at {spark_remote}... ({e})")
        time.sleep(5)
    
logging.info("Defining Schema for incoming data...")

schema = StructType([
    StructField("User_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("time", DoubleType(), True)
])

logging.info("Reading from Kafka topic...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", config.KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()
    
logging.info("Kafka Stream loaded successfully...")
json_df = df.selectExpr("CAST(value AS STRING) as json_string")

parsed_df = json_df.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")

fraud_df = parsed_df.withColumn(
    "fraud_flag",
    when(col("amount") > 3000, lit("Fraud")).otherwise(lit("Normal"))
)

alerts_df = fraud_df.filter(col("fraud_flag") == "Fraud")

kafka_output_df = alerts_df.select(to_json(struct("*")).alias("value"))

query = kafka_output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "fraud-alerts-topic") \
    .option("checkpointLocation", "/opt/app/checkpoints/fraud_alerts") \
    .trigger(processingTime='5 seconds') \
    .option("truncate", True) \
    .start()

query.awaitTermination()