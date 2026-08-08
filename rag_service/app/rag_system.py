import clickhouse_connect
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List, Dict, Tuple
import json
import os
import ollama

class IndustrialRAG:
    def __init__(self):
        clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        clickhouse_user = os.getenv('CLICKHOUSE_USER', 'rag_app_user')
        clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', '')
        clickhouse_db = os.getenv('CLICKHOUSE_DB', 'industrial_analytics')

        if not clickhouse_user or not clickhouse_password:
            print("WARNING: Using empty password or default user. Please configure secure credentials in .env file.")

        self.client = clickhouse_connect.get_client(
            host=clickhouse_host,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_db
        )
        # Use an industrial-aware embedding model (forced to CPU due to PyTorch sm_61 incompatibility)
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        # Or for better industrial domain: 'intfloat/e5-mistral-7b-instruct'
        
    def query_with_rag(self, 
                       query: str, 
                       equipment_ids: List[str] = None,
                       time_range: Tuple = None,
                       top_k: int = 5) -> Dict:
        """Execute a RAG query against the industrial data"""
        
        # Input Validation
        if len(query) > 1000:
            raise ValueError("Query exceeds maximum allowed length.")
        if top_k > 50:
            top_k = 50 # Prevent excessive retrieval
            
        # 1. Embed the query
        query_embedding = self.embedder.encode(query).tolist()
        
        # 2. Build the search query with filtering
        where_conditions = ["embedding[1] != 0.0"]
        if equipment_ids:
            where_conditions.append(f"equipment_id IN ({','.join([f'{repr(e)}' for e in equipment_ids])})")
        if time_range:
            where_conditions.append(f"time_range.1 >= '{time_range[0]}'")
            where_conditions.append(f"time_range.2 <= '{time_range[1]}'")
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        # 3. Vector similarity search
        vector_search_query = f"""
        WITH {query_embedding} AS query_vec
        SELECT
            equipment_id,
            context_type,
            content,
            tags,
            metadata,
            cosineDistance(embedding, query_vec) AS similarity
        FROM rag_context
        {where_clause}
        ORDER BY similarity ASC
        LIMIT {top_k}
        """
        
        results = self.client.query(vector_search_query).result_rows
        
        # 4. Get additional time-series context if needed
        ts_results = []
        if results and 'time_series' not in [r[1] for r in results]:
            # Add recent time-series context
            equip_ids = list(set([r[0] for r in results]))
            ts_query = f"""
            SELECT 
                equipment_id,
                argMax(value, event_time) as latest_value,
                max(value) as max_value,
                min(value) as min_value,
                avg(value) as avg_value
            FROM tag_timeseries
            WHERE equipment_id IN ({','.join([f'{repr(e)}' for e in equip_ids])})
            AND event_time > now() - INTERVAL 1 HOUR
            GROUP BY equipment_id
            """
            ts_results = self.client.query(ts_query).result_rows
            
        # 5. Log the query for future optimization
        self._log_query(query, query_embedding, len(results))
        
        return {
            'query': query,
            'context': results,
            'time_series': ts_results
        }
    
    def _log_query(self, query, embedding, context_count):
        """Log query for monitoring and optimization"""
        self.client.query(f"""
            INSERT INTO rag_query_history 
            (query_text, query_embedding, context_used, created_at)
            VALUES
            ({repr(query)}, {embedding}, {repr([f"context_{i}" for i in range(context_count)])}, now())
        """)

    def batch_generate_embeddings(self, batch_size=1000):
        """Generate embeddings in batches for new data"""
        # Get records without embeddings
        records = self.client.query(f"""
            SELECT context_id, content FROM rag_context
            WHERE length(embedding) = 0
            LIMIT {batch_size}
        """).result_rows
        
        if records:
            # Batch encode
            embeddings = self.embedder.encode([r[1] for r in records])
            
            # Update in batches
            updates = []
            for (context_id, _), embedding in zip(records, embeddings):
                updates.append([embedding.tolist(), context_id])
            
            for u in updates:
                self.client.query(f"ALTER TABLE rag_context UPDATE embedding = {u[0]} WHERE context_id = '{u[1]}'")
                
    def generate_insight(self, query: str, rag_results: Dict) -> str:
        """Use an LLM (Ollama) to generate an insight based on the retrieved context"""
        ollama_host = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
        ollama_model = os.getenv('OLLAMA_MODEL', 'mistral')
        
        # Format the context for the LLM
        context_str = "--- Retrieved Database Context ---\n"
        for idx, ctx in enumerate(rag_results.get('context', [])):
            context_str += f"[{idx+1}] Source: {ctx[1]} (Equipment {ctx[0]})\n{ctx[2]}\n\n"
            
        if rag_results.get('time_series'):
            context_str += "--- Recent Real-time Data ---\n"
            for ts in rag_results['time_series']:
                context_str += f"Equipment: {ts[0]}, Latest Value: {ts[1]}, Max: {ts[2]}, Min: {ts[3]}, Avg: {ts[4]}\n"
                
        prompt = f"""You are an expert industrial plant operator and efficiency consultant.
Your goal is to answer the user's query accurately using ONLY the provided context. If the context does not contain the answer, say so. Do not invent information.

User Query: {query}

{context_str}

Please provide a concise, actionable analysis:"""
        
        try:
            client = ollama.Client(host=ollama_host)
            response = client.chat(model=ollama_model, messages=[
                {'role': 'user', 'content': prompt}
            ])
            return response['message']['content']
        except Exception as e:
            return f"Error calling Ollama API: {str(e)}"
