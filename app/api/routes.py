from fastapi import APIRouter
from ..models.query import Query
from app.services.lineageService import extract_lineage_and_tables
router = APIRouter()

@router.post("/query")
def process_query(query: Query):
    result = extract_lineage_and_tables(query.sql)
    return result
