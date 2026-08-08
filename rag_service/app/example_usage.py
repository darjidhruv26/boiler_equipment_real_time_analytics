from rag_system import IndustrialRAG
from dotenv import load_dotenv

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Initialize RAG system
    print("Initializing IndustrialRAG...")
    try:
        rag = IndustrialRAG()
        
        print("\nQuerying for equipment issues...")
        # Query for equipment issue
        result = rag.query_with_rag(
            query="What are the abnormal pressure readings in the main pump over the last hour?",
            equipment_ids=['P001', 'P002', 'P003'],
            # time_range=('2026-08-02 09:00:00', '2026-08-02 10:00:00') # Optional
        )

        # Display results
        print("\n=== Context Retrieved ===")
        if not result['context']:
            print("No relevant context found. (Ensure embeddings are populated and data exists)")
        else:
            for context in result['context']:
                print(f"Equipment: {context[0]}")
                print(f"Context Type: {context[1]}")
                print(f"Content: {context[2]}")
                print(f"Similarity: {context[5]}\n")
                
        print("\n=== Time Series Context ===")
        print(result['time_series'])
        
    except Exception as e:
        print(f"Error executing RAG query: {e}")
        print("Please ensure ClickHouse is running and the schema is deployed.")

if __name__ == "__main__":
    main()
