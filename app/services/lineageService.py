import json
from sqllineage.runner import LineageRunner
from ..services.tableDetails import get_table_details, create_table_api,create_edge
from ..services.addLineageEdgeService import addLineage
from config import fqn_prefix
from ..models.table import Table, Column


def extract_lineage_and_tables(sql: str):
    columnsLineage=[]

    try:
        columnsLineage = []
        lineage = LineageRunner(sql=sql, verbose=True)
        col_lineage = lineage.get_column_lineage()
        

        for col in col_lineage:
            src = str(col[0])  # left column FQN
            tgt = str(col[1])  # right column FQN

            if "<default>" in src:
                src = src.replace("<default>", fqn_prefix)

            if "<default>" in tgt:
                tgt = tgt.replace("<default>", fqn_prefix)

            columnsLineage.append({"fromColumns": src, "toColumn": tgt})

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
            # asset = await get_asset_by_fqn(asset_fqn=fromtable)
            table_payload["edge"]["fromEntity"]["id"] = fromtable
            # asset = await get_asset_by_fqn(asset_fqn=totable)
            table_payload["edge"]["toEntity"]["id"] = totable
            
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
                # create_edge(payload)
                print("payloads",payload)   
            except Exception as e:
                print(e)
        return {"data":payloads}
    except Exception as e:
        print(f"Overall lineage extraction error: {e}")
  