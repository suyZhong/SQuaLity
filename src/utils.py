from enum import Enum
import logging
import re
import hashlib
import math
import pandas as pd
from copy import copy


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
    ECHO = 2
    SOURCE = 3
    ENABLE_INFO = 4
    DISABLE_INFO = 5


class StopRunnerException(Exception):
    pass


class DBEngineExcetion(Exception):
    def __init__(self, message):
        super().__init__(message)


DBMS_Set = set(['mysql', 'sqlite', 'postgresql',
               'duckdb', 'cockroachdb', 'psql'])
Suite_Set = set(['mysql', 'sqlite', 'postgresql',
                'duckdb', 'cockroachdb', 'squality'])

Running_Stats = ['success_file_num',
                 'total_sql',
                 'total_executed_sql',
                 'failed_statement_num',
                 'success_query_num',
                 'failed_query_num',
                 'wrong_query_num',
                 'wrong_stmt_num',
                 'statement_num',
                 'query_num',
                 'control_num',
                 'filter_sql',
                 ]


TestCaseColumns = ['INDEX',  # testcase index
                   'TYPE',  # Enum Type: Statement, Query and Control
                   'SQL',  # SQL string
                   'STATUS',  # Expected SQL execution status, 1 for Success, 2 for Failed
                   'RESULT',  # SQL execution result. For Statement it's error msg if fail
                   'DBMS',  # The dbms that can execute this test case
                   'SUITE',  # The suite that original test case came from
                   'INPUT_DATA',  # The input data for this test case
                   'DATA_TYPE',  # Query only, store the require result type
                   'SORT_TYPE',  # Query only, store the required sort methods
                   'LABEL',      # Query only, store the result label
                   'RES_FORM',   # Query only, store the result format
                   'IS_HASH',    # Query only, store the result is hash or not
                   ]

ResultColumns = ['DBMS_NAME', 'TESTFILE_INDEX', 'TESTFILE_PATH', 'ORIGINAL_SUITE',
                 'TESTCASE_INDEX', 'SQL', 'CASE_TYPE', 'EXPECTED_RESULT',
                 'ACTUAL_RESULT', 'EXEC_TIME', 'DATE', 'IS_ERROR', 'ERROR_MSG', 'LOGS_INDEX']

TESTCASE_PATH = {
    'postgresql': 'postgresql_tests/regress/'
}

DBMS_MAPPING = {
    'mysql': 'mysql',
    'sqlite': 'sqlite',
    'duckdb': 'duckdb',
    'cockroach': 'cockroachdb',
    'cockroachdb': 'cockroachdb',
    'postgres': 'postgresql',
    'postgresql': 'postgresql',
    'psql': 'postgresql',
}

SETUP_PATH = {
    'postgresql': 'postgresql_tests/regress/sql/test_setup.sql',
    'filter': 'data/flaky',
}

OUTPUT_PATH = {
    'testcase_dir': 'data/',
    'base': 'output/',
    'execution_result': 'output/{}_results.csv',  # means execution db engine
    'execution_log': 'output/{}_logs.csv'
}


class Record:

    def __init__(self, sql="", result="", suite="", input_data="", **kwargs) -> None:
        self.sql = sql
        self.result = result
        self.db = DBMS_Set
        self.id = kwargs['id']
        self.suite = suite
        self.input_data = input_data

    def set_execute_db(self, db: set):
        self.db = db


class Statement(Record):
    def __init__(self, sql="", result="", status=True,
                 affected_rows=0, input_data="",  **kwargs) -> None:
        super().__init__(sql, result, input_data=input_data, ** kwargs)
        self.status = status
        self.affected_rows = affected_rows


class Query(Record):
    def __init__(self, sql="", result="", data_type="I",
                 sort=SortType.NO_SORT, label="", res_format=ResultFormat.VALUE_WISE, input_data="", is_hash=True, **kwargs) -> None:
        super().__init__(sql=sql, result=result, input_data=input_data, **kwargs)
        self.data_type = data_type
        self.sort = sort
        self.label = label
        self.is_hash = is_hash
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


def convert_testfile_name(path: str, dbms: str):
    return "-".join(path.removeprefix(
        "{}_tests/".format(dbms)).replace(".test", ".csv").replace(".sql", ".csv").split('/'))


def convert_postgres_result(result: str):
    '''
    The function convert the expected result in postgres to the more general form in SQuaLity.
    '''
    # convert the SELECT result to the SQuaLity row-wise format
    # print(result)
    rows_regex = re.compile(r"\(\s*[0-9]+\s*rows?\)")
    result_lines = result.rstrip().split('\n')
    # if it is an error
    if result == "":
        return result
    if result_lines[0].strip().startswith('ERROR'):
        return "\n".join(result_lines)
    elif re.match(rows_regex, result_lines[-1]):
        # print(result_lines)
        result_rows = int(
            re.search(r"[0-9]+", result_lines[-1]).group())
        value_table = result_lines[-result_rows - 1:-1]
        # handle multiple rows
        if len(value_table) != result_rows:
            # empty_ind = [i for i, row in enumerate(value_table) if row.strip().endswith('+')]
            logging.warning(
                "the len of value table should be same with result_rows")

        # assert len(value_table) == result_rows, "the len of value table should be same with result_rows"
        row_wise_result_list = [
            [item.strip() for item in row.split('|')] for row in value_table]
        row_wise_result_list = [
            ['True' if elem == 't' else elem for elem in row] for row in row_wise_result_list]
        row_wise_result_list = [
            ['False' if elem == 'f' else elem for elem in row] for row in row_wise_result_list]

        row_wise_result = '\n'.join(['\t'.join(row)
                                    for row in row_wise_result_list])
        if result_rows > 0:
            return row_wise_result
        else:
            return ""
    else:
        # logging.warning("Parsing result warning: while parsing {}".format(result))
        return result


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

    def value_wise_compare(self, results, record, hash_threshold, is_hash=True):
        result_string = ""
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
        if is_hash and result_len > hash_threshold:
            result_string = self.hash_results(result_string)
            result_string = str(result_len) + \
                " values hashing to " + result_string
        cmp_flag = result_string.strip() == record.result.strip()
        return cmp_flag, result_string

    def row_wise_compare(self, results, record: Record):
        expected_result_list = record.result.strip().split('\n') if record.result else []
        # expected_result_list.sort()
        NULL = None
        actually_result_list = copy(results)
        # actually_result_list.sort()
        my_debug("%s, %s", actually_result_list, expected_result_list)
        if len(expected_result_list) == len(actually_result_list) == 0:
            cmp_flag = True
        elif len(expected_result_list) != len(actually_result_list):
            cmp_flag = False
        else:
            for i, row in enumerate(expected_result_list):
                items = row.strip().split('\t')
                for j, item in enumerate(items):
                    # direct comparison
                    rvalue = actually_result_list[i][j]
                    # my_debug("lvalue = [%s], rvalue = [%s]",item, rvalue)
                    cmp_flag = item is rvalue
                    cmp_flag = item == str(rvalue) or cmp_flag
                    # if DuckDB
                    cmp_flag = item == '(empty)' and rvalue == '' or cmp_flag
                    if not cmp_flag:
                        try:
                            lvalue = eval(item)
                        except (TypeError, SyntaxError, NameError):
                            continue
                        cmp_flag = lvalue == rvalue or cmp_flag
                        # if numeric (No, even data type is I, still would have float type
                        if type(lvalue) is float and type(rvalue) is float:
                            cmp_flag = math.isclose(
                                lvalue, rvalue) or cmp_flag
                if not cmp_flag:
                    break
        result_string = '\n'.join(['\t'.join(
            [str(item) if item != None else 'NULL' for item in row]) for row in results])
        return cmp_flag, result_string

    def cast_result_list(self, results: str, old, new):
        return [row.replace(old, new) for row in results]
