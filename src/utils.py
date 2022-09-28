from enum import Enum


class SortType(Enum):
    NoSort = 1
    RowSort = 2
    ValueSort = 3


class SLTKeywords(Enum):
    query = 1
    statement = 2


DBMS_Set = set(['mysql', 'sqlite', 'postgresql', 'duckdb', 'cockroachdb'])

class Record:
    
    def __init__(self, sql="", result="", db=DBMS_Set) -> None:
        self.sql = sql
        self.result = result
        self.db = db
        
    def set_execute_db(self, db=set()):
        self.db = db


class Statement(Record):
    def __init__(self, sql="", result="",db=DBMS_Set, status=True, affected_rows=0) -> None:
        super().__init__(sql=sql, result=result, db=db)
        self.status = status
        self.affected_rows = affected_rows


class Query(Record):
    def __init__(self, sql="", result="",db=DBMS_Set, data_type="I", sort=SortType.NoSort, label="") -> None:
        super().__init__(sql=sql, result=result,db=db)
        self.data_type = data_type
        self.sort = sort
        self.label = label
        


class Control(Record):
    def __init__(self, action="") -> None:
        super().__init__()
        self.action = action
