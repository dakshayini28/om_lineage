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
    # print('column_lineage_list',column_lineage_list)
    
    target_fqn = ".".join(columnsLineage[0]["toColumn"].split(".")[:-1])
    # print('columnsLineage',columnsLineage)
    source_fqns=[]
    for item in columnsLineage:
        for col in item['fromColumns']:
            source_fqn ='.'.join(col.split(".")[:-1])
            print('source_fqns',source_fqns)
            source_fqns.append(source_fqn)
            
    
    first_source_fqn = list(source_fqns)[0]
    # print('first_source_fqn',first_source_fqn)
    # print("results",results["tables"][first_source_fqn])
    source_id = results["tables"][first_source_fqn]["id"]
    target_id = results["tables"][target_fqn]["id"]

    from_entity = EntityRef(id=source_id)
    # print("from_entity",from_entity)
    to_entity = EntityRef(id=target_id)
    # print("to_entity",to_entity)
    lineage_details = LineageDetails(columnsLineage=column_lineage_list)
    # print("lineage_details",lineage_details)
    edge = Edge(
        fromEntity=from_entity, 
        toEntity=to_entity, 
        lineageDetails=lineage_details
    )
    
    edge_request = EdgeRequest(edge=edge)
    # print("edge_request",edge)
    # print(json.dumps(edge_request.model_dump(), indent=2))
    data= json.dumps(edge_request.model_dump(), indent=2)
    # print("data",data["edge"])
    # print("________________")
    # print(data)
    
    return data

    # except Exception as e:
    #     # Include collected data in the error response for debugging
    #     return {
    #         "error": f"Failed to construct EdgeRequest model: {str(e)}", 
    #         "collected_data": {
    #             "tables": results.get("tables", {}), 
    #             "columnsLineage": columnsLineage
    #         }
    #     }
    
    