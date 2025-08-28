"""
Vercel API handler for FastAPI backend
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import os
import logging
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Simple Text-to-Query API",
    description="Convert natural language to SQL queries and execute on Supabase",
    version="2.0.0"
)

# CORS middleware - Updated for Vercel deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://refract.looms.live",
        "https://refract-backend.vercel.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Import backend modules
try:
    from gemini_sql import GeminiTextToSQL
    from supabase_manager import SupabaseManager
except ImportError as e:
    logger.error(f"Failed to import backend modules: {e}")
    # Create dummy classes for now
    class GeminiTextToSQL:
        def generate_sql(self, query): return f"SELECT 'Error importing GeminiTextToSQL' as message"
        def explain_sql(self, sql): return "Module import failed"
    
    class SupabaseManager:
        async def execute_sql_query(self, sql, limit): 
            return [{"error": "Failed to import SupabaseManager", "message": "Check backend dependencies"}]

# Initialize services
supabase_manager = SupabaseManager()
gemini_sql = GeminiTextToSQL()

# Pydantic models
class SimpleQueryRequest(BaseModel):
    """Simple query request"""
    query: str = Field(..., description="Natural language query")
    max_results: int = Field(default=10, description="Maximum results to return")
    explain: bool = Field(default=True, description="Include AI explanation of the SQL")

class SimpleQueryResponse(BaseModel):
    """Simple query response"""
    success: bool
    query: str
    generated_sql: str = ""
    results: List[Dict[str, Any]]
    explanation: Optional[str] = None
    execution_time: float
    row_count: int
    error: Optional[str] = None

# Helper Functions

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Text-to-Query API is running",
        "endpoints": ["/health", "/query", "/simple-query"],
        "version": "2.0.0",
        "status": "active"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "simple-text-to-query"
    }

async def process_text_query(request: SimpleQueryRequest) -> SimpleQueryResponse:
    """
    Process text-to-query logic for both endpoints
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"Processing query: {request.query}")
        generated_sql = gemini_sql.generate_sql(request.query)
        logger.info(f"Generated SQL using Gemini: {generated_sql}")

        # If Gemini fails to generate a valid SQL, return error
        if not generated_sql or "ERROR:" in generated_sql or "Failed to generate" in generated_sql:
            return SimpleQueryResponse(
                success=False,
                query=request.query,
                generated_sql=generated_sql,
                results=[],
                execution_time=(datetime.now() - start_time).total_seconds(),
                row_count=0,
                error="Gemini could not generate a valid SQL for this query."
            )

        # Execute the query directly on Supabase
        results = await supabase_manager.execute_sql_query(generated_sql, request.max_results)
        execution_time = (datetime.now() - start_time).total_seconds()

        response = SimpleQueryResponse(
            success=True,
            query=request.query,
            generated_sql=generated_sql,
            results=results,
            execution_time=execution_time,
            row_count=len(results)
        )

        # Add explanation if requested and AI is available
        if request.explain:
            try:
                explanation = gemini_sql.explain_sql(generated_sql)
                response.explanation = explanation
            except:
                response.explanation = "AI explanation temporarily unavailable"

        logger.info(f"Query executed successfully - {len(results)} rows in {execution_time:.2f}s")
        return response

    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"Query processing failed: {str(e)}")
        return SimpleQueryResponse(
            success=False,
            query=request.query,
            generated_sql="",
            results=[],
            execution_time=execution_time,
            row_count=0,
            error=str(e)
        )

@app.post("/simple-query")
async def simple_text_to_query(request: SimpleQueryRequest) -> SimpleQueryResponse:
    """
    Simple text-to-query endpoint that works directly with Supabase
    No complex configuration required - perfect for testing!
    """
    return await process_text_query(request)

@app.post("/query")
async def text_to_query(request: SimpleQueryRequest) -> SimpleQueryResponse:
    """
    Text-to-query endpoint for generating SQL from natural language
    and performing Supabase operations
    """
    return await process_text_query(request)

# This is the handler that Vercel will call
handler = app
