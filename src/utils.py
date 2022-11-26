from enum import Enum
import logging
import hashlib
import pandas as pd


class SortType(Enum):
    NO_SORT = 1
    ROW_SORT = 2
    VALUE_SORT = 3


class ResultFormat(Enum):
    VALUE_WISE = 1
    ROW_WISE = 2
    HASH = 3


class RecordType(Enum):
    STATEMENT = 1
    QUERY = 2
    CONTROL = 3


class RunnerAction(Enum):
    HALT = 1


class StopRunnerException(Exception):
    pass


DBMS_Set = set(['mysql', 'sqlite', 'postgresql', 'duckdb', 'cockroachdb'])
Suite_Set = set(['mysql', 'sqlite', 'postgresql',
                'duckdb', 'cockroachdb', 'squality'])

Running_Stats = ['total_sql',
                 'failed_statement_num',
                 'success_query_num',
                 'failed_query_num',
                 'wrong_query_num',
                 'wrong_stmt_num',
                 'statement_num',
                 'query_num']


TestCaseColumns = ['INDEX',  # testcase index
                   'TYPE',  # Enum Type: Statement, Query and Control
                   'SQL',  # SQL string
                   'STATUS',  # Expected SQL execution status, 1 for Success, 2 for Failed
                   'RESULT',  # SQL execution result. For Statement it's error msg if fail
                   'DBMS',  # The dbms that can execute this test case
                   'SUITE', # The suite that original test case came from
                   'DATA_TYPE',  # Query only, store the require result type
                   'SORT_TYPE',  # Query only, store the required sort methods
                   'LABEL',      # Query only, store the result label
                   'RES_FORM',   # Query only, store the result format
                   ]

ResultColumns = ['DBMS_NAME', 'TESTFILE_INDEX', 'TESTFILE_PATH', 'ORIGINAL_SUITE',
                 'TESTCASE_INDEX', 'SQL', 'CASE_TYPE', 'EXPECTED_RESULT',
                 'ACTUAL_RESULT', 'EXEC_TIME', 'DATE', 'IS_ERROR', 'LOGS_INDEX']

OUTPUT_PATH = {
    'testcase_dir': 'data/',
    'execution_result': 'output/{}_results.csv', # means execution db engine
    'execution_log': 'output/{}_logs.csv'
}


class Record:

    def __init__(self, sql="", result="", suite="", **kwargs) -> None:
        self.sql = sql
        self.result = result
        self.db = DBMS_Set
        self.id = kwargs['id']
        self.suite = suite

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

    def set_resformat(self, res_format: ResultFormat):
        self.res_format = res_format


class Control(Record):
    def __init__(self, sql="", result="",
                 action=RunnerAction.HALT, **kwargs) -> None:
        super().__init__(sql, result, **kwargs)
        self.action = action


def my_debug(mystr: str, *args):
    logging.debug(mystr, *args)


class ResultHelper():
    def __init__(self, results, record: Record) -> None:
        self.results = results
        self.record = record

    def hash_results(self, results: str):
        """hash the result string

            Args:
                results (str): a string of results

            Returns:
                _type_: md5 hash value string
            """
        return hashlib.md5(results.encode(encoding='utf-8')).hexdigest()

    def int_format(self, item):
        try:
            item = int(item)
        except ValueError:
            if pd.isna(item):
                return "NULL"
            item = 0
        except TypeError:  # when the element is None
            return "NULL"
        return "%d" % item

    def float_format(self, item):
        if pd.isna(item):
            return "NULL"
        try:
            item = float(item)
        except ValueError:  # When the element is long string
            item = 0.0
        except TypeError:  # when the element is None
            return "NULL"
        return "%.3f" % item

    def text_format(self, item):
        if pd.isna(item):
            return 'NULL'
        return str(item)

    def format_results(self, results, datatype: str):
        cols = list(datatype)
        tmp_results = pd.DataFrame(results)
        # tmp_results = tmp_results.fillna('NULL')
        for i, col in enumerate(cols):
            if col == "I":
                tmp_results[i] = tmp_results[i].apply(self.int_format)
            elif col == "R":
                tmp_results[i] = tmp_results[i].apply(self.float_format)
            elif col == "T":
                tmp_results[i] = tmp_results[i].apply(self.text_format)
            else:
                logging.warning("Datatype not support")
        return tmp_results.values.tolist()

    def sort_result(self, results, sort_type=SortType.ROW_SORT):
        """sort the result (rows of the results)
        Args:
            results (A list, each item is a tuple)): results list
            sort_type (sort type, optional): . Defaults to SortType.RowSort.
        Returns:
            str: A str of results
        """
        result_flat = []
        if sort_type == SortType.ROW_SORT:
            results = [list(map(str, row)) for row in results]
            results.sort()
            for row in results:
                for item in row:
                    result_flat.append(item+'\n')
        elif sort_type == SortType.VALUE_SORT:
            for row in results:
                for item in row:
                    result_flat.append(str(item)+'\n')
            result_flat.sort()
        else:
            for row in results:
                for item in row:
                    result_flat.append(str(item)+'\n')
        # myDebug(result_flat)
        return ''.join(result_flat)

    def value_wise_compare(self, results, record, hash_threshold):
        if results:
            result_len = len(results) * len(results[0])
            # Format the result by the query command para
            results_fmt = self.format_results(
                results=results, datatype=record.data_type)
            # sort result and output flat list
            # myDebug(results_fmt)
            result_string = self.sort_result(
                results_fmt, sort_type=record.sort)
        else:
            result_len = 0
        if hash_threshold and result_len > hash_threshold:
            result_string = self.hash_results(result_string)
            result_string = str(result_len) + \
                " values hashing to " + result_string
        cmp_flag = result_string.strip() == record.result.strip()
        return cmp_flag, result_string

    def cast_result_list(self, results: str, old, new):
        return [row.replace(old, new) for row in results]
