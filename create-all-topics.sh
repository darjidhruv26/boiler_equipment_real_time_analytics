#!/bin/bash

# Network name from your docker-compose
NETWORK="industrial_equipment_real_time_analytics_monitoring"
KAFKA_BOOTSTRAP="kafka:9092"

echo "Creating Kafka topics using temporary container..."
echo "Network: $NETWORK"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo ""

# Function to create topic
create_topic() {
    local topic=$1
    local partitions=$2
    
    echo "Creating topic: $topic (partitions: $partitions)"
    
    docker run --rm \
        --network $NETWORK \
        -e KAFKA_JMX_PORT= \
        apache/kafka:3.7.0 \
        /opt/kafka/bin/kafka-topics.sh --create \
            --bootstrap-server $KAFKA_BOOTSTRAP \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor 1 \
            --config cleanup.policy=delete \
            --config retention.ms=604800000 \
            --config compression.type=snappy \
            --config segment.bytes=1073741824 \
            --config min.insync.replicas=1 2>&1 | grep -v "WARNING"
    
    if [ $? -eq 0 ]; then
        echo "✓ Topic $topic created successfully"
    else
        echo "✗ Topic $topic may already exist or error occurred"
    fi
    echo ""
}

# Create all topics
create_topic "boiler-sensors" 10
create_topic "turbine" 14
create_topic "generator" 10
create_topic "condenser" 10
create_topic "cooling-tower" 14
create_topic "coal-handling" 7
create_topic "ash-handling" 7
create_topic "water-treatment" 10
create_topic "electrical-system" 9
create_topic "instrumentation-control" 13
create_topic "grid-carbon-intensity" 1

echo "========================================="
echo "All topics created! Verifying..."
echo "========================================="

# List all topics
docker run --rm \
    --network $NETWORK \
    -e KAFKA_JMX_PORT= \
    apache/kafka:3.7.0 \
    /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server $KAFKA_BOOTSTRAP

echo ""
echo "Done! You can now run your Flink job and producer."