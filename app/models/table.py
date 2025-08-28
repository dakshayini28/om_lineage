from pydantic import BaseModel
from typing import List, Dict, Optional

class Column(BaseModel):
    name: str
    dataType: str
    dataLength: Optional[int] = None

class Table(BaseModel):
    name: str
    tableType: str = "Regular"
    columns: List[Column]
    databaseSchema: str = "om_mysql_17.udp_db.udp_db"
    extension: Dict[str, str] = {"reviewed": "published"}
