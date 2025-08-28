from pydantic import BaseModel
from typing import List

class EntityRef(BaseModel):
    id: str
    type: str="table"

class ColumnLineage(BaseModel):
    fromColumns: List[str]
    toColumn: str

class LineageDetails(BaseModel):
    columnsLineage: List[ColumnLineage]

class Edge(BaseModel):
    fromEntity: EntityRef
    toEntity: EntityRef
    lineageDetails: LineageDetails

class EdgeRequest(BaseModel):
    edge: Edge
