import os
import logging
import random
import pandas as pd
from .utils import TestCaseColumns


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

            for test_file in test_files:
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

        ind = random.randint(0, self.test_num - length) if rand else 0
        return self.test_cases[column].loc[ind: ind + length]
    
    def get_statements(self, length:int = -1, rand: bool = False):
        df = self.get_data(['TYPE','SQL'])
        statements = df[df['TYPE'] == 'STATEMENT']
        return statements
    
    def get_queries(self, length: int = -1, rand: bool = False):
        df = self.get_data(['TYPE', 'SQL'])
        queries = df[df['TYPE'] == 'QUERY']
        return queries
