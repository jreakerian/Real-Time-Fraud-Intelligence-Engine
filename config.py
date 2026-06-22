import os

KAFKA_TOPIC = "fraud-topic"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

RAW_PATH = "data_lake/raw/"
PROCESSED_PATH = "data_lake/processed/"
ANALYTICS_PATH = "data_lake/analytics/"

INPUT_FILE = "sample_data/transactions.csv"