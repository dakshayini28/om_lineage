import json
from sqllineage.runner import LineageRunner
from ..services.tableDetails import get_table_details, create_table_api,create_edge
from ..services.addLineageEdgeService import addLineage
from config import fqn_prefix
from ..models.table import Table, Column
from fastapi import HTTPException


def extract_lineage_and_tables(sql: str):
    columnsLineage=[]

    try:
        columnsLineage = []
        lineage = LineageRunner(sql=sql, verbose=True)
        col_lineage = lineage.get_column_lineage()
        tables_to_process = {} 

        for col in col_lineage:
            src = str(col[0])  # left column FQN
            tgt = str(col[1])  # right column FQN

            if "<default>" in src:
                src = src.replace("<default>", fqn_prefix)

            if "<default>" in tgt:
                tgt = tgt.replace("<default>", fqn_prefix)

            columnsLineage.append({"fromColumns": src, "toColumn": tgt})
            for col_fqn in [src, tgt]:
                    table_fqn = ".".join(col_fqn.split(".")[:-1])
                    col_name = col_fqn.split(".")[-1]
                    
                    if table_fqn not in tables_to_process:
                        tables_to_process[table_fqn] = set() 
                    tables_to_process[table_fqn].add(col_name)
        
        tables = {}
        for cols in columnsLineage:
            fromcol = cols.get("fromColumns")
            tocol = cols.get("toColumn")
            totable = ".".join(tocol.split(".")[:-1])
            fromtable = ".".join(fromcol.split(".")[:-1])
            tables[fromtable] = totable

        payloads = []

        for fromtable, totable in tables.items():
            table_payload = {
                "edge": {
                    "fromEntity": {"id": "", "type": "table"},
                    "toEntity": {"id": "", "type": "table"},
                    "lineageDetails": {"columnsLineage": []},
                }
            }
            # # asset = await get_asset_by_fqn(asset_fqn=fromtable)
            # table_payload["edge"]["fromEntity"]["id"] = fromtable
            # # asset = await get_asset_by_fqn(asset_fqn=totable)
            # table_payload["edge"]["toEntity"]["id"] = totable
            
            # table_payload["edge"]["lineageDetails"]["columnsLineage"] = []
            # print(fromtable)
            
            
            # print("asset",asset)
            asset = get_table_details(fromtable)
            table_payload["edge"]["fromEntity"]["id"] = asset.get("id")
            for table_fqn, columns in tables_to_process.items():
                print(f"Processing table: {table_fqn} with columns: {columns}")
                if table_fqn != totable:
                    continue
                try:
                    asset = get_table_details(table_fqn)
                    table_payload["edge"]["toEntity"]["id"] = asset.get("id")
                    print(f"Table {table_fqn} found.")
                except HTTPException as e:
                    print(f"Table {table_fqn} not found. Attempting to create it.")
                    table_name = table_fqn.split(".")[-1]
                    columns_list = [Column(name=col_name, dataType="STRING") for col_name in columns] #giving fixed val
                    new_table_model = Table(
                        name=table_name,
                        columns=columns_list
                    )
                    print(new_table_model)
                    asset=create_table_api(new_table_model)
                    table_payload["edge"]["toEntity"]["id"] = asset.get("id")
            
            table_payload["edge"]["lineageDetails"]["columnsLineage"] = []
            for cols in columnsLineage:
                col_level_payload = {}
                fromcol = cols.get("fromColumns")
                tocol = cols.get("toColumn")
                fromcoltable = ".".join(fromcol.split(".")[:-1])
                tocoltable = ".".join(tocol.split(".")[:-1])
                if fromcoltable == fromtable:
                    col_level_payload["fromColumns"] = [fromcol]
                    if tocoltable == totable:
                        col_level_payload["toColumn"] = tocol
                if col_level_payload:
                    table_payload["edge"]["lineageDetails"]["columnsLineage"].append(
                        col_level_payload
                    )
            payloads.append(table_payload)
        for payload in payloads:
            try:
                print("payload",payload)
                create_edge(payload)
                print("payloads",payload)   
            except Exception as e:
                print(e)
        return {"data":payloads}
    except Exception as e:
        print(f"Overall lineage extraction error: {e}")
  