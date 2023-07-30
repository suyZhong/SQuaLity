import os
import sys
import re
import pandas as pd
from typing import List

from .utils import Control, Query, Record, Statement, TestCaseColumns, OUTPUT_PATH


class TestcaseCollector():
    def __init__(self, base_path=OUTPUT_PATH['testcase_dir']) -> None:
        self.columns = TestCaseColumns
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
            self.record_row['SUITE'] = record.suite
            self.record_row['INPUT_DATA'] = record.input_data
            if record_type is Statement:
                self.record_row['STATUS'] = str(record.status)
            elif record_type is Query:
                self.record_row['DATA_TYPE'] = record.data_type
                self.record_row['SORT_TYPE'] = record.sort.value
                # TODO if not str here would raise warnings
                self.record_row['STATUS'] = str(True)
                self.record_row['RES_FORM'] = record.res_format.value
                self.record_row['LABEL'] = record.label
                self.record_row['IS_HASH'] = record.is_hash
            elif record_type is Control:
                self.record_row['SQL'] = str(record.action.value)

            self.testcase_df = pd.concat(
                [self.testcase_df, pd.DataFrame([self.record_row])], ignore_index=True)

    def dump_to_csv(self):
        self.testcase_df.to_csv(self.output_path, mode='w', header=True)


def find_local_tests(db_name: str):
    db_name = db_name.lower()

    test_suite_dir = db_name + "_tests/"
    if db_name == "cockroachdb":
        test_suite_dir += 'logic_test'
    elif db_name == "duckdb":
        return find_local_duckdb_test()
    elif db_name == "mysql":
        return get_mysql_test()
    elif db_name == "postgresql":
        return get_postgresql_schedule_test()
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
    duckdb_test_regex = re.compile(".*\.test(_slow)?$")
    tests_files = []

    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        if path.find('csv') > 0:
            continue
        if path.find('sqlite') > 0:
            # print("file_list: ", file_list)
            continue
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list if re.match(duckdb_test_regex, file_name)]
    return tests_files

def get_postgresql_schedule_test():
    test_suite_dir = 'postgresql_tests/'
    schedule_tests_fn = test_suite_dir + 'regress/parallel_schedule'
    all_tests = []
    with open(schedule_tests_fn, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith('test: '):
                all_tests += line.split()[1:]
    return [f"{test_suite_dir}regress/sql/{test}.sql" for test in all_tests]

def get_mysql_test():
    test_suite_dir = 'mysql_tests/t'
    mysql_regex = re.compile(".*\.test$")
    tests_files = []

    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list if re.match(mysql_regex, file_name)]
    return tests_files
