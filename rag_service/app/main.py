from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv

# Import our RAG system
from rag_system import IndustrialRAG

# Load env variables
load_dotenv()

app = FastAPI(
    title="Industrial RAG API",
    description="Scalable RAG service for Industrial Equipment Analytics",
    version="1.0.0"
)

# Initialize the RAG service globally for the app
try:
    rag_service = IndustrialRAG()
except Exception as e:
    print(f"Failed to initialize RAG Service: {e}")
    rag_service = None

class QueryRequest(BaseModel):
    query: str
    equipment_ids: Optional[List[str]] = None
    top_k: Optional[int] = 5

class QueryResponse(BaseModel):
    query: str
    context_found: int
    llm_insight: str
    raw_context: list

@app.get("/health")
def health_check():
    """Simple health check endpoint"""
    return {"status": "healthy", "service": "Industrial RAG API"}

@app.post("/api/v1/query", response_model=QueryResponse)
def execute_query(request: QueryRequest):
    """
    Execute a RAG query to retrieve context and generate an LLM insight.
    """
    if not rag_service:
        raise HTTPException(status_code=500, detail="RAG Service is not initialized.")
        
    try:
        # 1. Retrieve Context
        retrieval_results = rag_service.query_with_rag(
            query=request.query,
            equipment_ids=request.equipment_ids,
            top_k=request.top_k
        )
        
        # 2. Generate Insight via LLM
        insight = rag_service.generate_insight(
            query=request.query,
            rag_results=retrieval_results
        )
        
        return QueryResponse(
            query=request.query,
            context_found=len(retrieval_results.get('context', [])),
            llm_insight=insight,
            raw_context=retrieval_results.get('context', [])
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
