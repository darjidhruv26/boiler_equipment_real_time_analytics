# Boiler Equipment Real-Time Analytics

This project demonstrates a real-time data analytics pipeline for monitoring industrial boiler equipment. It uses a Go-based simulator to produce sensor data, Kafka as a message broker, Flink for stream processing, and ClickHouse for storing and analyzing time-series data.

## Architecture

```
[Go Data Simulator] -> [Kafka Topic: boiler-sensors] -> [Apache Flink Job] -> [ClickHouse Database]
```

## Prerequisites

*   Docker & Docker Compose
*   Go (for the data producer)
*   Java & Maven (for the Flink job)

## Setup & Execution

Follow these steps to get the project up and running.

### 1. Start Services

This project should include a `docker-compose.yml` to run Kafka, Zookeeper, Flink, and ClickHouse. Start all services in detached mode.

```bash
docker-compose up -d
```

### 2. Create Kafka Topic

Execute the following command to create the Kafka topic with 10 partitions, which will be used for ingesting sensor data.

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic boiler-sensors --bootstrap-server localhost:9092 --partitions 10 --replication-factor 1
```

### 3. Setup ClickHouse

Connect to the ClickHouse container's interactive client to set up the database and tables.

```bash
docker exec -it clickhouse clickhouse-client --user admin --password admin
```

Inside the client, run the following SQL commands to create the database and the necessary tables for sensor metadata and time-series data.

```sql
CREATE DATABASE IF NOT EXISTS industrial_analytics;

USE industrial_analytics;

CREATE TABLE IF NOT EXISTS sensor_timeseries
(
    `event_time` DateTime,
    `pi_point_id` UInt32,
    `value` Float64,
    `quality` UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (pi_point_id, event_time);

CREATE TABLE IF NOT EXISTS sensor_metadata
(
    `pi_point_id` UInt32,
    `equipment` String,
    `tag_name` String,
    `unit` String,
    `description` String
)
ENGINE = MergeTree()
ORDER BY pi_point_id;
```

Next, populate the `sensor_metadata` table by pasting the entire content of the `insert.sql` file into the ClickHouse client. Once done, you can exit with `exit;`.

### 4. Build and Run the Flink Job

From the `flink-job` directory, build the Java project using Maven.

```bash
cd flink-job/
mvn clean package
cd ..
```

Copy the compiled JAR to the Flink JobManager container.

```bash
docker cp flink-job/target/flink-kafka-job-1.0.jar flink-jobmanager:/opt/flink/
```

Submit the job to the Flink cluster to start processing data from Kafka to ClickHouse.

```bash
docker exec flink-jobmanager flink run /opt/flink/flink-kafka-job-1.0.jar
```

### 5. Run the Data Producer

Finally, navigate to the `producer` directory and run the Go application. This will begin generating and sending simulated boiler sensor data to your Kafka topic.

```bash
cd boiler-pi-simulator/producer/
go run .
```

Your real-time pipeline is now active. You can query the `sensor_timeseries` and `sensor_metadata` tables in ClickHouse to verify that data is being ingested and to perform analytics.
