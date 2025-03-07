# End-to-End Realtime Data Streaming Project

## Table of Contents
- [End-to-End Realtime Data Streaming Project](#end-to-end-realtime-data-streaming-project)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [System Architecture](#system-architecture)
  - [Technologies](#technologies)
  - [Getting Started](#getting-started)

## Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

<!-- # Ref: https://www.youtube.com/watch?app=desktop&v=GqAcTrqKcrY, https://github.com/airscholar/e2e-data-engineering -->

## System Architecture

![System Architecture](img/architecture.png)

The project is designed with the following components:

- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker
- DataGrip

## Getting Started

```bash
git clone https://github.com/bdbao/etl-randuser.git
cd etl-randuser

chmod +x scripts/entrypoint.sh
docker compose up -d
```
- Run DAG in **Airflow UI** (http://localhost:8080, username/pw is `admin`). Waiting until the Scheduler ready to run.
- View Kafka Consumer:
  ```bash
  docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 --topic users_created --from-beginning
  ```
```bash
spark-submit --version # e.g: show version 2.12.18-3.5.4. 
# Then edit .config(...) of create_spark_connection() in spark_stream.py

pip install cassandra-driver # (if not installed)
spark-submit --master local[2] --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 scripts/spark_stream.py

open http://localhost:4040/StreamingQuery
```
- (Optional) Run in Spark Standalone Mode
  ```bash
  spark-submit --master spark://localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 scripts/spark_stream.py
  open http://localhost:9090
  ```
- Open **DataGrip** (or other tools) to view Cassandra Data (username/pw is `cassandra`).
  ```bash
  docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
      describe spark_streams.created_users;
      SELECT * FROM spark_streams.created_users;
  ```
- Remove all containers:
  ```bash
  docker compose down --volumes
  ```