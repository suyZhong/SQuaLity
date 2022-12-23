import os
import logging
import random
import pandas as pd
from tqdm import tqdm
from .utils import TestCaseColumns, ResultColumns, OUTPUT_PATH


class TestCaseAnalyzer():
    def __init__(self) -> None:
        self.test_cases = pd.DataFrame(columns=TestCaseColumns)
        self.test_num = 0
        self.attributes = set(TestCaseColumns)

    def read_testcase(self, test_file: str):
        compression = 'zip' if test_file.endswith('.zip') else None
        try:
            df = pd.read_csv(test_file, compression=compression,
                             na_filter=False, dtype=str).fillna('')
        except FileNotFoundError:
            logging.warning(
                "Test file %s not find or not in the correct form!", test_file)
            df = pd.DataFrame(columns=TestCaseColumns)
        return df

    def load_testcases(self, dir_name: str = "", file_name: str = ""):
        if file_name != "":
            self.test_cases = self.read_testcase(file_name)
        elif dir_name != "":
            test_files = []
            all_test = pd.DataFrame(columns=TestCaseColumns)
            try:
                g = os.walk(dir_name)
            except OSError:
                logging.warning("Wrong directory %s", dir_name)
                return pd.DataFrame(columns=TestCaseColumns)
            for path, _, file_list in g:
                test_files += [os.path.join(path, file) for file in file_list]

            for test_file in tqdm(test_files):
                df = self.read_testcase(test_file)
                all_test = pd.concat([all_test, df], ignore_index=True)
            self.test_cases = all_test
        else:
            logging.warning("dir_name and file_name could not be both empty!")
            self.test_cases = pd.DataFrame(columns=TestCaseColumns)
        self.test_num = len(self.test_cases)

    def get_results(self, length: int = 10, rand: bool = False):
        return self.get_data('RESULT', length=length, rand=rand)

    def get_data(self, column=[], length: int = -1, rand: bool = False):
        if type(column) is str:
            column = [column]
        column = [col.upper() for col in column]
        cols = set(column)
        assert cols.issubset(self.attributes)
        if length > self.test_num or length < 0:
            length = self.test_num
            return self.test_cases[column]

        ind = random.randint(0, self.test_num - length) if rand else 0
        return self.test_cases[column].loc[ind: ind + length]

    def get_statements(self, length: int = -1, rand: bool = False):
        df = self.get_data(self.attributes)
        statements = df[df['TYPE'] == 'STATEMENT']
        return statements

    def get_queries(self, length: int = -1, rand: bool = False):
        df = self.get_data(self.attributes)
        queries = df[df['TYPE'] == 'QUERY']
        return queries


class TestResultAnalyzer():
    def __init__(self) -> None:
        self.results = pd.DataFrame(columns=ResultColumns)
        self.logs = pd.DataFrame([])
        self.attributes = set(ResultColumns)
        self.result_num = 0

    def load_results(self, dbms: str):
        self.results = pd.read_csv(
            OUTPUT_PATH['execution_result'].format(dbms))
        self.logs = pd.read_csv(OUTPUT_PATH['execution_log'].format(dbms))
        self.result_num = len(self.results)

    def get_result_cols(self, df: pd.DataFrame = None, column=[], length: int = -1, rand: bool = False):
        if df is not None:
            result_df = df
        else:
            result_df = self.results
        if type(column) is str:
            column = [column]
        column = [col.upper() for col in column]
        cols = set(column)
        assert cols.issubset(self.attributes)
        if length > self.result_num or length < 0:
            length = self.result_num
            return result_df[column]

        ind = random.randint(0, self.result_num - length) if rand else 0
        return self.results[column].loc[ind: ind + length]

    def get_error_rows(self):
        return self.results.loc[self.results.IS_ERROR == True]
