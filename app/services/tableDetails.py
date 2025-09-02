import requests
import json
from config import open_meta_data_url,open_meta_data_token
from fastapi import HTTPException
from ..models.table import Table,Column
from ..models.lineageEdge import Edge


def get_table_details(fqn: str):
    url = f"{open_meta_data_url}/api/v1/tables/name/{fqn}"
    # print(url)
    resp = requests.get(url, headers={"Authorization":f"Bearer {open_meta_data_token}"})
    # print(resp.status_code)
    # print(resp.json())
    if resp.status_code == 200:
        return resp.json()
    else:
        raise HTTPException(status_code=404, detail="not found")
    

def create_table_api(new_table_model:Table):
    url = f"{open_meta_data_url}/api/v1/tables"
    resp = requests.post(url, headers={"Authorization":f"Bearer {open_meta_data_token}"},
                         json=new_table_model.model_dump())
    # print("res",resp)
    if resp.status_code == 201 or resp.status_code == 200:
        return resp.json()
    else:
       raise HTTPException(status_code=404, detail="table not found")
    

def create_edge(edge):
    # print("edge",edge)
    print("Payload being sent:", edge)

    url = f"{open_meta_data_url}/api/v1/lineage"
    resp = requests.put(url, headers={"Authorization":f"Bearer {open_meta_data_token}"},
                         json=edge)
    print("res",resp.text)
   

    if resp.status_code == 201 or resp.status_code == 200:
        return resp
    else:
        raise HTTPException(status_code=404,detail=resp.text)
     

    
    
