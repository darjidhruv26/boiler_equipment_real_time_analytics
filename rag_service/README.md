# Industrial RAG System for Carbon Reduction

This directory contains the Python service and ClickHouse SQL schema necessary to implement a RAG (Retrieval-Augmented Generation) system on top of your existing industrial telemetry data. The focus is specifically targeted towards analyzing and reducing carbon emissions by combining semantic knowledge with real-time operational context.

## Directory Layout
- `sql/`: Contains all schema modifications and new tables/materialized views.
  - `01_rag_schema.sql`: Modifies existing tables and creates standard RAG tables.
  - `02_carbon_schema.sql`: Sets up Carbon Knowledge tracking.
  - `03_materialized_views.sql`: Sets up views that automatically aggregate time-series data into textual context.
- `app/`: Contains the Python implementation of the RAG querying logic.
  - `rag_system.py`: The `IndustrialRAG` class implementation.
  - `example_usage.py`: Example script showing how to trigger a RAG query.

## Setup Instructions

### 1. Initialize ClickHouse Schema
Ensure your `clickhouse` container is running (`docker compose up -d`). Then apply the SQL scripts sequentially:
```bash
docker exec -i clickhouse clickhouse-client -n < sql/01_rag_schema.sql
docker exec -i clickhouse clickhouse-client -n < sql/02_carbon_schema.sql
docker exec -i clickhouse clickhouse-client -n < sql/03_materialized_views.sql
```

**Note:** The experimental vector similarity index is used. Make sure your ClickHouse version supports it, or enable it globally in your ClickHouse config (`allow_experimental_vector_similarity_index=1`).

### 2. Python Setup
Navigate to the `rag_service` directory and install the requirements:
```bash
cd rag_service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Running the Example
You can test the vector search and query functionality:
```bash
python app/example_usage.py
```
*(The first run will download the SentenceTransformer model).*
