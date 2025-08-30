import json
from sqllineage.runner import LineageRunner
from ..services.tableDetails import get_table_details, create_table_api,create_edge
from ..services.addLineageEdgeService import addLineage
from config import fqn_prefix
from ..models.table import Table, Column


def extract_lineage_and_tables(sql: str):
    results = {"tables": {}}
    columnsLineage=[]
    tables_to_process = {} 
    data={}

    try:
        lineage = LineageRunner(sql=sql, verbose=True)
        col_lineage = lineage.get_column_lineage()
        # print("hoii", col_lineage)

        for col in col_lineage:
            src = str(col[0])  # left column FQN
            tgt = str(col[1])  # right column FQN
            
            if "<default>" in src:
                src = src.replace("<default>", fqn_prefix)
            
            if "<default>" in tgt:
                tgt = tgt.replace("<default>", fqn_prefix)

            columnsLineage.append({"fromColumns": [src], "toColumn": tgt})

            for col_fqn in [src, tgt]:
                table_fqn = ".".join(col_fqn.split(".")[:-1])
                col_name = col_fqn.split(".")[-1]
                
                if table_fqn not in tables_to_process:
                    tables_to_process[table_fqn] = set() 
                tables_to_process[table_fqn].add(col_name)

        for table_fqn, columns in tables_to_process.items():
            print(f"Processing table: {table_fqn} with columns: {columns}")
            if table_fqn in results["tables"]:
                continue
            try:
                table_details = get_table_details(table_fqn)
                results["tables"][table_fqn] = table_details
                print(f"Table {table_fqn} found.")
            except Exception as e:
                print(f"Table {table_fqn} not found. Attempting to create it.")
                table_name = table_fqn.split(".")[-1]
                columns_list = [Column(name=col_name, dataType="STRING") for col_name in columns] #giving fixed val
                new_table_model = Table(
                    name=table_name,
                    columns=columns_list
                )
                results["tables"][table_fqn] = create_table_api(new_table_model)
                # print(new_table_model)
        
        data= addLineage(results,columnsLineage)
        # print("data",data)
        create_edge(data)
    except Exception as e:
        data=results["error"] = f"Lineage extraction failed: {str(e)}"
        print(f"Overall lineage extraction error: {e}")
    return {
            "status": "success",
            "data": json.loads(data)
            }