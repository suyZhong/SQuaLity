import os
import sys
import re
import pandas as pd
from typing import List

from .utils import Control, Query, Record, Statement


class TestcaseCollector():
    def __init__(self, base_path="data/") -> None:
        self.columns = ['INDEX',  # testcase index
                        'TYPE',  # Enum Type: Statement, Query and Control
                        'SQL',  # SQL string
                        'STATUS',  # SQL execution status, 1 for Success, 2 for Failed
                        'RESULT',  # SQL execution result. For Statement it's error msg if fail
                        'DBMS',  # TODO not sure if we need to split it in different schemas/tables
                        'DATA_TYPE',  # Query only, store the require result type
                        'SORT_TYPE',  # Query only, store the required sort methods
                        'RES_FORM',   # Query only, store the result format
                        ]
        self.testcase_df = pd.DataFrame(columns=self.columns)
        self.testcase_name = ""
        self.record_row = {}.fromkeys(self.columns)
        self.base_path = base_path
        self.output_path = base_path + "demo.zip"

    def init_testcase_schema(self, testsuite_name: str, testcase_name: str, compression: bool = True):
        self.testcase_name = testcase_name
        self.output_path = "{}{}/{}".format(
            self.base_path, testsuite_name, testcase_name)
        if compression:
            self.output_path += ".zip"

    def save_records(self, records: List[Record]):
        self.testcase_df = pd.DataFrame(columns=self.columns)
        for record in records:
            record_type = type(record)
            self.record_row = {}.fromkeys(self.columns)
            self.record_row['INDEX'] = record.id
            self.record_row['TYPE'] = record_type.__name__.upper()
            self.record_row['SQL'] = record.sql
            self.record_row['DBMS'] = ",".join(record.db)
            self.record_row['RESULT'] = record.result
            if record_type is Statement:
                self.record_row['STATUS'] = str(record.status)
            elif record_type is Query:
                self.record_row['DATA_TYPE'] = record.data_type
                self.record_row['SORT_TYPE'] = record.sort.value
                # TODO if not str here would raise warnings
                self.record_row['STATUS'] = str(True)
                self.record_row['RES_FORM'] = record.res_format.value
            elif record_type is Control:
                self.record_row['SQL'] = str(record.action.value)

            self.testcase_df = pd.concat(
                [self.testcase_df, pd.DataFrame([self.record_row])], ignore_index=True)

    def dump_to_csv(self):
        self.testcase_df.to_csv(self.output_path, mode='w', header=True)


def find_local_tests(db_name: str):
    db_name = db_name.lower()

    test_suite_dir = db_name + "_tests/"
    if db_name == "cockroach":
        test_suite_dir += 'testdata/logic_test'
    elif db_name == "duckdb":
        return find_local_duckdb_test()
    elif db_name == "mysql":
        test_suite_dir += 'r'
    elif db_name == "postgres":
        test_suite_dir += 'regress/expected'
    elif db_name == "sqlite":
        test_suite_dir += ''
    else:
        sys.exit("test suite not supported yet")

    tests_files = []
    print("walk in " + test_suite_dir)
    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list]
    return tests_files


def find_local_duckdb_test():
    test_suite_dir = 'duckdb_tests/'
    duckdb_test_regex = re.compile(".*\.test")
    tests_files = []
    
    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list if re.match(duckdb_test_regex,file_name)]
    return tests_files
