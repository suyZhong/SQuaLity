from enum import Enum
import logging
import hashlib


class SortType(Enum):
    NO_SORT = 1
    ROW_SORT = 2
    VALUE_SORT = 3

class ResultFormat(Enum):
    VALUE_WISE = 1
    ROW_WISE = 2

class RecordType(Enum):
    STATEMENT = 1
    QUERY = 2
    CONTROL = 3

class RunnerAction(Enum):
    HALT = 1


class StopRunnerException(Exception):
    pass


DBMS_Set = set(['mysql', 'sqlite', 'postgresql', 'duckdb', 'cockroachdb'])
Suite_Set = set(['mysql', 'sqlite', 'postgresql', 'duckdb', 'cockroachdb', 'squality'])

Running_Stats = ['total_sql',
                 'failed_statement_num',
                 'success_query_num',
                 'failed_query_num',
                 'wrong_query_num',
                 'wrong_stmt_num',
                 'statement_num',
                 'query_num']


class Record:

    def __init__(self, sql="", result="", **kwargs) -> None:
        self.sql = sql
        self.result = result
        self.db = DBMS_Set
        self.id = kwargs['id']

    def set_execute_db(self, db: set):
        self.db = db


class Statement(Record):
    def __init__(self, sql="", result="", status=True,
                 affected_rows=0, **kwargs) -> None:
        super().__init__(sql, result, **kwargs)
        self.status = status
        self.affected_rows = affected_rows


class Query(Record):
    def __init__(self, sql="", result="", data_type="I",
                 sort=SortType.NO_SORT, label="", res_format=ResultFormat.VALUE_WISE, **kwargs) -> None:
        super().__init__(sql=sql, result=result, **kwargs)
        self.data_type = data_type
        self.sort = sort
        self.label = label
        self.res_format = res_format
        
    def set_resformat(self, res_format:ResultFormat):
        self.res_format = res_format


class Control(Record):
    def __init__(self, sql="", result="",
                 action=RunnerAction.HALT, **kwargs) -> None:
        super().__init__(sql, result, **kwargs)
        self.action = action


def my_debug(mystr: str, *args):
    logging.debug(mystr, *args)


def hash_results(results: str):
    """hash the result string

        Args:
            results (str): a string of results

        Returns:
            _type_: md5 hash value string
        """
    return hashlib.md5(results.encode(encoding='utf-8')).hexdigest()
