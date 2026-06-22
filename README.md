# Real-Time Fraud Intelligence Engine

## A Learning Project for Understanding Real-Time Streaming Architectures

This project was built as a hands-on learning platform to understand **real-time data streaming**, **event-driven architectures**, and the **Lambda Architecture pattern**. It simulates a fraud detection system that processes financial transactions in real-time.

---

## What This Project Teaches

### Core Concepts Demonstrated

1. **Real-Time Stream Processing** - How to process data as it arrives, not in batches
2. **Event-Driven Architecture** - How services communicate through events/messages
3. **Lambda Architecture** - Combining batch and speed layers for comprehensive data processing
4. **Containerization** - Using Docker to orchestrate multiple services
5. **Distributed Computing** - Apache Spark cluster for parallel processing

---

## Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Data Generator │────▶│    Kafka     │────▶│  Spark Streaming│
│   (Producer)    │     │  (Message    │     │  (Real-Time     │
│                 │     │   Broker)    │     │   Processing)   │
└─────────────────┘     └──────────────┘     └─────────────────┘
                               │                      │
                               ▼                      ▼
                        ┌──────────────┐     ┌─────────────────┐
                        │   Kafdrop    │     │  Alert Output   │
                        │    (UI)      │     │  (Fraud Alerts) │
                        └──────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Spark Batch │
                        │  Processing  │
                        └──────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka | Real-time event streaming |
| **Stream Processing** | Apache Spark (Structured Streaming) | Real-time computation |
| **Batch Processing** | Apache Spark (Batch Mode) | Historical analysis |
| **Containerization** | Docker & Docker Compose | Service orchestration |
| **UI/Monitoring** | Kafdrop | Visualize Kafka topics |
| **Language** | Python 3.10 | Application code |

---

## Project Structure

```
.
├── config.py                 # Centralized configuration
├── docker-compose.yml        # Service orchestration
├── data_generator/
│   ├── producer.py          # Generates fake transaction data
│   ├── Dockerfile           # Container definition
│   └── requirements.txt     # Python dependencies
└── spark_jobs/
    ├── fraud_stream.py      # Real-time fraud detection
    ├── spark_batch.py       # Batch data processing
    ├── fraud_aggregation.py # Analytics and aggregation
    └── checkpoints/         # Spark streaming checkpoints
```

---

## How It Works

### 1. Data Generation (`producer.py`)
- Generates fake financial transactions every 2 seconds
- Each transaction includes: user_id, amount, location, timestamp
- Publishes messages to Kafka topic `fraud-topic`

### 2. Real-Time Processing (`fraud_stream.py`)
- Reads transactions from Kafka using Spark Structured Streaming
- Applies fraud detection rules (amount > $3000 = Fraud)
- Writes alerts to `fraud-alerts-topic` every 5 seconds

### 3. Batch Processing (`spark_batch.py`)
- Reads historical transaction data from CSV
- Processes and stores results in Parquet format
- Demonstrates the "batch layer" of Lambda Architecture

### 4. Analytics (`fraud_aggregation.py`)
- Aggregates data by location
- Calculates metrics like total transactions and average amounts
- Shows how to derive business insights from processed data

---

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available for containers

### Running the System

```bash
# Start Kafka, Spark, and the streaming processor
docker-compose up -d

# View logs
docker-compose logs -f fraud-stream-processor

# Access Kafdrop UI (visualize topics)
# Open browser: http://localhost:9000

# Access Spark Master UI
# Open browser: http://localhost:8080
```

### Running Batch Jobs

```bash
# Run batch processing manually
docker-compose run spark-batch-ingest

# Run analytics aggregation
docker-compose run spark-analytics-gold
```

### Stopping Everything

```bash
docker-compose down
```

---

## Key Learning Points

### Understanding Kafka
- **Topics**: Categories for messages (like `fraud-topic` and `fraud-alerts-topic`)
- **Producers**: Services that publish messages
- **Consumers**: Services that read and process messages
- **Brokers**: Kafka servers that store and route messages

### Understanding Spark Streaming
- **Structured Streaming**: Scale SQL queries to stream processing
- **Micro-batch Execution**: Process streams in small batches (5 seconds here)
- **Checkpointing**: Save state to recover from failures

### Understanding Docker Networking
- Services communicate via service names (e.g., `kafka:29092`)
- Port mapping exposes services to your host machine
- Internal vs external listeners for Kafka

---

## Configuration

Edit `config.py` to customize:
- Kafka topic names
- Bootstrap server addresses
- Data lake paths

Edit `docker-compose.yml` to:
- Change resource allocations
- Add new services
- Modify network settings

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Spark can't connect | Check `SPARK_REMOTE` environment variable |
| Kafka connection refused | Wait for Kafka to fully start (check logs) |
| Checkpoint errors | Delete `spark_jobs/checkpoints/` directory |
| Out of memory | Increase Docker memory limit |

---

## Next Steps for Learning

1. **Modify fraud detection rules** - Add more sophisticated detection logic
2. **Add new metrics** - Track velocity, location changes, etc.
3. **Implement windowing** - Calculate metrics over time windows
4. **Add a dashboard** - Connect Grafana or similar for visualization
5. **Scale up** - Add more Spark workers for parallelism

---

## Notes

This is a **learning project** designed to demonstrate real-time streaming concepts. In production, you would add:
- Authentication and authorization
- Schema validation (Schema Registry)
- Exactly-once processing guarantees
- Monitoring and alerting infrastructure
- Proper error handling and retry logic

---

## License

This project is for educational purposes.