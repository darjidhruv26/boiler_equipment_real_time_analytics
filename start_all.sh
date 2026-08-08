#!/bin/bash

# Save the base directory so we can always return to it
BASE_DIR=$(pwd)

echo "🚀 Starting Industrial RAG Pipeline..."

# 1. Start core infrastructure (Kafka, ClickHouse, Flink)
echo "📦 Starting core services (Kafka, ClickHouse, Flink)..."
docker compose up -d

# Wait for services to be ready
echo "⏳ Waiting for ClickHouse to initialize..."
while ! docker exec clickhouse clickhouse-client -q "SELECT 1" > /dev/null 2>&1; do
    sleep 2
done

# 2. Start the Flink stream processing job
echo "🌊 Submitting Flink Job..."
cd "$BASE_DIR/flink-job" || exit
# Note: if you change your java code, run `mvn clean package` here first!
docker cp target/flink-kafka-job-1.0.jar flink-jobmanager:/opt/flink/
if curl -s http://localhost:8081/jobs | grep -q '"status":"RUNNING"'; then
    echo "⚡ A Flink job is already running. Skipping submission to prevent duplicates."
else
    docker exec -d flink-jobmanager flink run -d /opt/flink/flink-kafka-job-1.0.jar
fi
cd "$BASE_DIR" || exit

# 3. Start the Data Simulator
echo "🏭 Starting boiler sensor simulator..."
cd "$BASE_DIR/boiler-pi-simulator" || exit
# Check if container exists and is stopped, otherwise run a new one
if docker ps -a --format '{{.Names}}' | grep -Eq "^power-plant-producer$"; then
    docker start power-plant-producer
else
    docker run -d --rm --name power-plant-producer \
        --network industrial_equipment_real_time_analytics_monitoring \
        -e KAFKA_BROKER=kafka:9092 \
        power-plant-producer:latest
fi
cd "$BASE_DIR" || exit

# 4. Start Ollama Local LLM
echo "🧠 Starting local Ollama AI model (mistral)..."
# Check if container exists and is stopped, otherwise run a new one
if docker ps -a --format '{{.Names}}' | grep -Eq "^ollama$"; then
    docker start ollama
else
    docker run -d --gpus=all -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
fi
# Ensure mistral is pulled (this is fast if already downloaded)
docker exec -d ollama ollama pull mistral

# 5. Start the FastAPI RAG Service
echo "🌐 Starting FastAPI RAG Service..."
cd "$BASE_DIR/rag_service" || exit
source venv/bin/activate
cd app || exit
# Start uvicorn in the background and log to a file
CUDA_VISIBLE_DEVICES="" OLLAMA_MODEL=mistral nohup uvicorn main:app --reload > rag_api.log 2>&1 &
cd "$BASE_DIR" || exit

echo ""
echo "✅ All systems are starting up!"
echo "➡️  RAG API Swagger UI: http://127.0.0.1:8000/docs"
echo "➡️  RAG API Logs: View with 'tail -f rag_service/app/rag_api.log'"
echo "➡️  ClickHouse Database: localhost:8123 (admin/admin)"
