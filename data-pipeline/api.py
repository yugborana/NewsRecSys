import logging
from fastapi import FastAPI, Query
from typing import List, Optional, Dict
from pydantic import BaseModel
from vector_store import VectorStore

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('news_api')

app = FastAPI(title="News Search API")
vector_store = VectorStore()

class SearchResult(BaseModel):
    id: str
    score: float
    title: str
    summary: str
    url: str
    source: str
    category: Optional[str] = None

class SearchResponse(BaseModel):
    results: List[SearchResult]
    count: int
    query: str

@app.get("/search", response_model=SearchResponse)
async def search(
    query: str = Query(..., description="Search query"),
    limit: Optional[int] = Query(5, description="Number of results to return"),
    source: Optional[str] = Query(None, description="Filter by news source"),
    category: Optional[str] = Query(None, description="Filter by category")
):
    """Search for news articles by semantic similarity"""
    logger.info(f"Search request: query='{query}', limit={limit}, source={source}, category={category}")
    
    # Prepare filters if provided
    filters = {}
    if source:
        filters['source'] = source
    if category:
        filters['category'] = category
    
    # Perform search
    results = vector_store.search_similar(
        query_text=query,
        limit=limit,
        filters=filters if filters else None
    )
    
    # Format response
    formatted_results = []
    for result in results:
        formatted_results.append(
            SearchResult(
                id=result.get('id', ''),
                score=float(result.get('score', 1.0)),
                title=result.get('title', ''),
                summary=result.get('summary', ''),
                url=result.get('url', ''),
                source=result.get('source', ''),
                category=result.get('category', None)
            )
        )
    
    return SearchResponse(
        results=formatted_results,
        count=len(formatted_results),
        query=query
    )

@app.get("/sources")
async def list_sources():
    """List all available news sources"""
    from config import NEWS_SOURCES
    return {"sources": NEWS_SOURCES}

@app.get("/categories")
async def list_categories():
    """List all available categories"""
    from config import DEFAULT_TOPICS
    return {"categories": DEFAULT_TOPICS}

@app.get("/health")
async def health_check():
    """API health check endpoint"""
    return {"status": "ok", "service": "news_search_api"}