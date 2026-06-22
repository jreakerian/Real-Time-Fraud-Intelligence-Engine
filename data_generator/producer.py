import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random
import config

logging.basicConfig(level=logging.INFO)

logging.info("Starting Kafka Producer...")

producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            value_serializer = lambda v: json.dumps(v).encode('utf-8'),
            # Explicitly setting api_version helps avoid handshake issues during startup
            api_version=(0, 10, 1)
        )
        logging.info("Connected to Kafka successfully.")
    except (NoBrokersAvailable, Exception) as e:
        logging.info(f"Waiting for Kafka broker to be available... ({e})")
        time.sleep(5)

while True:
    data = {
        "user_id": random.randint(1, 1000),
        "amount": random.randint(10, 5000),
        "location": random.choice(["US","CMR","UK"]),
        "time": time.time()
    }
    
    producer.send(config.KAFKA_TOPIC, data)
    logging.info(f"Produced data: {data}")
    
    time.sleep(2)