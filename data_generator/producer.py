import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
import json
import time
import random
import config

logging.basicConfig(level=logging.INFO)

logging.info("Starting Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

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