from enum import Enum
import logging
import re
import hashlib
import math
from decimal import Decimal
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
    REGEX = 4


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


def timeout_handler(signum, frame):
    raise TimeoutError("Timed out!")


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
    return "-".join(path.replace(".test", ".csv").replace(".sql", ".csv").split('/')[1:])


def convert_postgres_result(result: str):
    '''
    The function convert the expected result in postgres to the more general form in SQuaLity.
    '''
    # convert the SELECT result to the SQuaLity row-wise format
    # print(result)
    rows_regex = re.compile(r"\(\s*[0-9]+\s*rows?\)")
    result_lines = result.rstrip().split('\n')
    is_query = False
    # if it is an error
    if result == "":
        return result, is_query
    if result_lines[0].strip().startswith('ERROR'):
        return "\n".join(result_lines), is_query
    # elif re.match(rows_regex, result_lines[-1]):
    elif (index := next((i for i, row in enumerate(result_lines) if re.match(rows_regex, row)), None)) is not None:
        # print(result_lines)
        is_query = True
        result_rows = int(
            re.search(r"[0-9]+", result_lines[index]).group())
        value_table = result_lines[index - result_rows:index]
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
            return row_wise_result, is_query
        else:
            return "", is_query
    else:
        logging.warning(
            "Parsing result warning: while parsing {}".format(result))
        result = result.replace('\n\n', '\n')
        return result, is_query


class ResultHelper():
    def __init__(self, results, record: Record) -> None:
        self.results = results
        self.record = record
        if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
            self.debug("""@#$%""")

    def debug(self, ptrn: str):
        if self.record.sql.find(ptrn) != -1:
            my_debug(self.record.sql)
            my_debug(self.record.result)
            my_debug(self.results)

    def hash_results(self, results: str):
        """hash the result string

            Args:
                results (str): a string of results

            Returns:
                _type_: md5 hash value string
            """
        return hashlib.md5(results.encode(encoding='utf-8')).hexdigest()

    def int_format(self, item):
        if isinstance(item, int):
            return str(int(item))
        elif item == None:
            return "NULL"
        else:
            try:
                return str(int(item))
            except ValueError:
                return "0"

    def float_format(self, item):
        if isinstance(item, float):
            return "%.3f" % item
        elif isinstance(item, str):
            try:
                return "%.3f" % float(item)
            except:
                return "0.000"
        elif isinstance(item, Decimal):
            return "%.3f" % float(item)
        elif isinstance(item, int):
            return "%.3f" % float(item)
        elif item == None:
            return "NULL"
        else:
            return "0.000"

    def text_format(self, item):
        return str(item) if item != None else "NULL"

    def format_results(self, results, datatype: str):
        cols = list(datatype)
        format_func = {'I': self.int_format,
                       'R': self.float_format, 'T': self.text_format}
        results = [[format_func[col](item) for col, item in zip(
            cols, row)] for row in results]
        return results

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

    def _row_wise_compare_str(self, actual_results, expected_results):
        NULL = None
        if len(expected_results) == len(actual_results) == 0:
            cmp_flag = True
        elif len(expected_results) != len(actual_results):
            cmp_flag = False
        else:
            for i, row in enumerate(expected_results):
                items = row.strip().split('\t')
                # my_debug("%s, %s", str(items), str(actual_results[i]))
                if len(items) != len(actual_results[i]):
                    cmp_flag = False
                    break
                for j, item in enumerate(items):
                    # direct comparison
                    rvalue = actual_results[i][j]
                    if type(rvalue) is str:
                        rvalue = rvalue.replace('\0', '\\0')
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
            [str(item).replace('\0', '\\0') if item != None else 'NULL' for item in row]) for row in actual_results])
        return cmp_flag, result_string

    def row_wise_compare(self, results, record: Record):
        # the result is just what we want.
        if type(results) == str:
            if results == record.result:
                return True, results
            else:
                return False, results
        expected_result_list = record.result.strip().split('\n') if record.result else []
        cmp_flag = False
        actual_result_list = copy(results)
        # actually_result_list.sort()
        # sort the actual result list based on the string
        my_debug("%s, %s", actual_result_list, expected_result_list)
        cmp_flag, result_string = self._row_wise_compare_str(
            actual_result_list, expected_result_list)
        # if the result is same, just end here
        if cmp_flag:
            return cmp_flag, result_string
        # if not, try to sort the result and do again
        expected_result_list.sort()
        actual_result_list = sorted(actual_result_list, key=str)
        cmp_flag, result_string = self._row_wise_compare_str(
            actual_result_list, expected_result_list)
        return cmp_flag, result_string

    def regex_compare(self, results, record: Record):
        cmp_flag = False
        if len(results) != 0:
            result_string = '\n'.join(['\t'.join(
                [str(item).replace('\0', '\\0') if item != None else 'NULL' for item in row]) for row in results])
            want_match = record.result.find('<REGEX>') != -1
            regex = re.sub(r'.*<REGEX>:', '', record.result) if want_match else re.sub(r'.*<\!REGEX>:', '', record.result)
            my_debug("regex = %s", regex)
            if want_match:
                cmp_flag = re.search(regex, result_string, re.DOTALL) != None
            else:
                cmp_flag = re.search(regex, result_string, re.DOTALL) == None
        else:
            result_string = ''
        return cmp_flag, result_string

    def cast_result_list(self, results: str, old, new):
        return [row.replace(old, new) for row in results]
