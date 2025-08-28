from fastapi import FastAPI
from config import open_meta_data_url
from .models.table import Table
from .models.lineageEdge import Edge
from .models.query import Query
app=FastAPI()

@app.get("/")
def fun():
    return open_meta_data_url

@app.post("/postTable")
def add(data: Table): 
    return f"{data} added"

@app.post("/postLineage")
def add(data: Edge): 
    return f"{data} added"

@app.post("/query")
def query(query:Query):
    return query