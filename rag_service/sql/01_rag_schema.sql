SET allow_experimental_inverted_index = 1;
USE industrial_analytics;

-- Add embeddings to store semantic representations of equipment context
ALTER TABLE equipment_tree 
ADD COLUMN IF NOT EXISTS embedding Array(Float32) DEFAULT [],
ADD COLUMN IF NOT EXISTS description_text String DEFAULT '',
ADD COLUMN IF NOT EXISTS metadata String DEFAULT '{}'; -- JSON for additional attributes

-- Create vector index for similarity search
SET allow_experimental_usearch_index = 1;
ALTER TABLE equipment_tree 
ADD INDEX IF NOT EXISTS embedding_idx embedding TYPE usearch('L2Distance') GRANULARITY 1;

-- Full-text index for equipment names and descriptions
SET allow_experimental_inverted_index = 1;
ALTER TABLE equipment_tree 
ADD INDEX IF NOT EXISTS description_idx description_text TYPE inverted GRANULARITY 1;

-- Store aggregated context for RAG queries
CREATE TABLE IF NOT EXISTS rag_context
(
    context_id UUID DEFAULT generateUUIDv4(),
    equipment_id String,
    context_type String, -- 'hierarchy', 'time_series', 'alert', 'maintenance'
    content String,
    embedding Array(Float32),
    time_range Tuple(DateTime, DateTime),
    tags Array(String),
    metadata String DEFAULT '{}',
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(created_at)
ORDER BY (equipment_id, context_id)
SETTINGS index_granularity = 8192;

-- Vector index for context search
ALTER TABLE rag_context 
ADD INDEX IF NOT EXISTS context_embedding_idx embedding TYPE usearch('cosineDistance') GRANULARITY 1;

-- Track RAG queries for performance monitoring
CREATE TABLE IF NOT EXISTS rag_query_history
(
    query_id UUID DEFAULT generateUUIDv4(),
    query_text String,
    query_embedding Array(Float32),
    equipment_filter Array(String),
    time_filter Tuple(DateTime, DateTime),
    response_text String,
    context_used Array(String),
    latency_ms UInt32,
    token_count UInt32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(created_at)
ORDER BY (created_at)
SETTINGS index_granularity = 8192;

-- Cache frequent queries
CREATE TABLE IF NOT EXISTS rag_cache
(
    query_hash String,
    query_text String,
    cached_response String,
    created_at DateTime DEFAULT now(),
    expires_at DateTime DEFAULT now() + INTERVAL 1 HOUR
)
ENGINE = MergeTree
ORDER BY (query_hash)
TTL expires_at;

-- Monitor RAG performance
CREATE VIEW IF NOT EXISTS rag_performance_monitor AS
SELECT
    toStartOfHour(created_at) AS hour,
    count(*) AS query_count,
    avg(latency_ms) AS avg_latency,
    avg(token_count) AS avg_tokens
FROM rag_query_history
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;
