from enum import Enum


class SortType(Enum):
    NoSort = 1
    RowSort = 2
    ValueSort = 3


class SLTKeywords(Enum):
    query = 1
    statement = 2


class Record:
    def __init__(self, sql="", result="") -> None:
        self.sql = sql
        self.result = result


class Statement(Record):
    def __init__(self, sql="", result="", status=True, affected_rows=0) -> None:
        super().__init__(sql=sql, result=result)
        self.status = status
        self.affected_rows = affected_rows


class Query(Record):
    def __init__(self, sql="", result="", data_type="I", sort=SortType.NoSort, label="") -> None:
        super().__init__(sql=sql, result=result)
        self.data_type = data_type
        self.sort = sort
        self.label = label
        


class Control(Record):
    def __init__(self, action="") -> None:
        super().__init__()
        self.action = action
