import clickhouse_connect
import os
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

def main():
    print("Loading environment variables...")
    load_dotenv()
    
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    clickhouse_user = os.getenv('CLICKHOUSE_USER', 'admin')
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', 'admin')
    clickhouse_db = os.getenv('CLICKHOUSE_DB', 'industrial_analytics')

    print("Connecting to ClickHouse...")
    client = clickhouse_connect.get_client(
        host=clickhouse_host, 
        username=clickhouse_user, 
        password=clickhouse_password, 
        database=clickhouse_db
    )
    
    print("Loading embedding model (this may take a moment)...")
    embedder = SentenceTransformer('all-MiniLM-L6-v2')
    
    print("Fetching the latest context for all equipment...")
    # Fetch the most recent, non-embedded context string for each unique equipment
    query = """
    SELECT 
        equipment_id, 
        argMax(context_type, created_at),
        argMax(content, created_at), 
        argMax(tags, created_at)
    FROM rag_context
    WHERE embedding[1] = 0.0
    GROUP BY equipment_id
    """
    records = client.query(query).result_rows
    
    if not records:
        print("No new equipment data found to process.")
        return
        
    print(f"Found {len(records)} unique equipment units. Generating vectors...")
    
    # Extract the text content to embed
    texts = [r[2] for r in records]
    
    # Generate vectors in one batch
    embeddings = embedder.encode(texts)
    
    # Prepare data for bulk insert
    insert_data = []
    for row, embedding in zip(records, embeddings):
        equipment_id = row[0]
        context_type = row[1]
        content = row[2]
        tags = row[3]
        
        insert_data.append([equipment_id, context_type, content, embedding.tolist(), tags])
        
    print("Saving vector embeddings back to ClickHouse...")
    client.insert(
        'rag_context', 
        insert_data, 
        column_names=['equipment_id', 'context_type', 'content', 'embedding', 'tags']
    )
    
    print("✅ Successfully generated and saved embeddings for all equipment!")

if __name__ == "__main__":
    main()
