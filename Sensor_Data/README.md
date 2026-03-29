# Data Schema and Project Execution Guide

This document provides detailed instructions for setting up the database schema, Kafka topics, and running the complete real-time analytics pipeline.

## Prerequisites

*   Docker & Docker Compose
*   Go (for the data producer)
*   Java & Maven (for the Flink job)
*   Python 3 (for data conversion script)

## Setup & Execution

### Step 1: Start Core Infrastructure

Ensure all necessary services (Kafka, Zookeeper, Flink, ClickHouse) are running. From the project root directory:

```bash
docker-compose up -d
```

### Step 2: Create Kafka Topics

The data producer sends sensor data to various Kafka topics based on the equipment type. Execute the following commands to create all the required topics.

```bash
# For Boiler data
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic boiler-sensors \
  --partitions 10 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824


# For Turbine data
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic turbine \
  --partitions 14 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824

# Create the Generator topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic generator \
  --partitions 10 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Condenser topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic condenser \
  --partitions 10 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Cooling Tower topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic cooling-tower \
  --partitions 14 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Coal Handling topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic coal-handling \
  --partitions 7 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Ash Handling topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic ash-handling \
  --partitions 7 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Water Treatment topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic water-treatment \
  --partitions 10 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Electrical System topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic electrical-system \
  --partitions 9 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2

# Create the Instrumentation & Control topic with partitions for each equipment category
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic instrumentation-control \
  --partitions 13 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2 1
```


### Step 3: Setup ClickHouse Schema

Connect to the ClickHouse container to create the database and tables.

1.  **Create the Database**:

    ```bash
    docker exec -it clickhouse clickhouse-client
    ```

    Inside the client, create the database:

    ```sql
    CREATE DATABASE IF NOT EXISTS industrial_analytics;
    USE industrial_analytics;
    ```
    You can now exit the client.

2.  **Create Tables and Materialized View**:

    The schema is defined in `schema.sql` and `tag_1min_mv.sql`. Execute these files to create the necessary tables.

    ```bash
    # Create the main tables
    cat schema.sql | docker exec -i clickhouse clickhouse-client -d industrial_analytics --multiquery

    # Create the materialized view for aggregation
    cat tag_1min_mv.sql | docker exec -i clickhouse clickhouse-client -d industrial_analytics --multiquery
    ```

    You can verify the tables were created:
    ```bash
    docker exec -it clickhouse clickhouse-client -d industrial_analytics -q "SHOW TABLES;"
    ```

### Step 4: Build and Run the Flink Job

The Flink job processes data from Kafka and ingests it into ClickHouse.

1.  **Navigate to the Flink job directory and build the project**:

    ```bash
    cd ../flink-job/
    mvn clean package
    ```

2.  **Copy the JAR to the Flink container and run the job**:

    ```bash
    # Copy the JAR file
    docker cp target/flink-kafka-job-1.0.jar flink-jobmanager:/opt/flink/

    # Execute the Flink job in detached mode
    docker exec -it flink-jobmanager flink run -d /opt/flink/flink-kafka-job-1.0.jar
    ```

### Step 5: Run the Data Producer

Finally, start the Go-based data simulator to generate and send sensor data to Kafka.

1.  **Navigate to the producer directory**:

    ```bash
    cd ../boiler-pi-simulator/producer/
    ```

2.  **Run the producer**:

    ```bash
    go run .
    ```

Your real-time data pipeline is now active. You can query the tables in the `industrial_analytics` database in ClickHouse to see the data flowing in.