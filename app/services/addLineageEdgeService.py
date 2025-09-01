import requests
import json
from sqllineage.runner import LineageRunner
from ..services.tableDetails import get_table_details, create_table_api
from config import fqn_prefix
from ..models.table import Table, Column
from pydantic import BaseModel
from typing import List, Dict, Optional, Set
from ..models.lineageEdge import ColumnLineage, LineageDetails, EntityRef, Edge, EdgeRequest

def addLineage(results, columnsLineage):
    if not columnsLineage:
        return {"tables": results["tables"]}

    column_lineage_list = [
        ColumnLineage(fromColumns=item["fromColumns"], toColumn=item["toColumn"])
        for item in columnsLineage
    ]
    
    target_fqn = ".".join(columnsLineage[0]["toColumn"].split(".")[:-1])
    source_fqns=[]
    for item in columnsLineage:
        for col in item['fromColumns']:
            source_fqn ='.'.join(col.split(".")[:-1])
            print('source_fqns',source_fqns)
            source_fqns.append(source_fqn)
            
    
    edges = []
    for source_fqn in set(source_fqns):   # avoid duplicates
        source_id = results["tables"][source_fqn]["id"]
        target_id = results["tables"][target_fqn]["id"]

        from_entity = EntityRef(id=source_id)
        to_entity = EntityRef(id=target_id)

        lineage_details = LineageDetails(
            columnsLineage=[
                cl for cl in column_lineage_list 
                if any(col.startswith(source_fqn) for col in cl.fromColumns)
            ]
        )

        edge = Edge(
            fromEntity=from_entity,
            toEntity=to_entity,
            lineageDetails=lineage_details
        )

        edge_request = EdgeRequest(edge=edge)
        edges.append(edge_request.model_dump())
    # print("edge_request",edge)
    # print(json.dumps(edge_request.model_dump(), indent=2))
    data= json.dumps(edge_request.model_dump(), indent=2)
    # print("data",data["edge"])
    # print("________________")
    # print(data)
    
    return data
    
    