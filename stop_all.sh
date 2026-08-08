#!/bin/bash

echo "🛑 Stopping Industrial RAG Pipeline..."

echo "🛑 Stopping Flink and core services..."
docker compose down

echo "🛑 Stopping Boiler Sensor Simulator..."
docker stop power-plant-producer

echo "🛑 Stopping Ollama AI model..."
docker stop ollama

echo "🛑 Stopping FastAPI RAG Service..."
pkill -f "uvicorn main:app"

echo "✅ All systems completely stopped!"
