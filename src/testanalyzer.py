import os
import logging
import random
import sqlparse
import re
from sqlparse.sql import Identifier
import pandas as pd
from tqdm import tqdm
from copy import copy
from .utils import TestCaseColumns, ResultColumns, OUTPUT_PATH, convert_testfile_name, DBMS_MAPPING


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from fuzzywuzzy import fuzz


def compute_similarity(a: str, b: str):
    # use fuzzywuzzy to compute similarity
    return fuzz.ratio(a, b)


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
                df['TESTFILE_PATH'] = test_file
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
    
    def get_sql_statement_type(self, sql:str):
        if sql.lstrip().startswith('\\'):
            return 'CLI_COMMAND'
        if not sql.strip():
            print("Error: Empty SQL")
            return None
        try:
            parsed = sqlparse.parse(sql)
        except Exception as e:
            print(f"Error: {e} in SQL {sql}")
            return None
        try:
            statement = parsed[0]
        except IndexError:
            print(f"Error: No statement found in SQL {sql}")
            return None
        first_token = statement.tokens[0]
        if first_token.ttype is sqlparse.tokens.Keyword.DDL:
            second_token = statement.tokens[2]  # In "CREATE TABLE", TABLE is the second token (index 2) after whitespace
            return first_token.value.upper() + " "+ second_token.value.upper()
        if first_token.ttype in sqlparse.tokens.Keyword:
            return first_token.value.upper()
        else:
            print(f"Error: Unknown statement type in SQL {sql}")
            return first_token.value.split()[0].upper()


class TestResultAnalyzer():
    DDL = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'COMMENT', 'RENAME']
    DML = ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'UPSERT', 'REPLACE']
    TCL = ['COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE', 'SET', 'BEGIN']

    def __init__(self) -> None:
        self.results = pd.DataFrame(columns=ResultColumns)
        self.logs = pd.DataFrame([])
        self.errors = pd.DataFrame([])
        self.attributes = set(ResultColumns)
        self.dbms_suite = ""
        self.result_path = ""
        self.result_num = 0

    def load_results(self, dbms: str, dir_name: str = ""):
        self.dbms_suite = DBMS_MAPPING[dbms]
        if dir_name:
            self.results_path = os.path.join(
                dir_name, OUTPUT_PATH['execution_result'].format(dbms).split('/')[1])
            logs_path = os.path.join(
                dir_name, OUTPUT_PATH['execution_log'].format(dbms).split('/')[1])
        else:
            self.results_path = OUTPUT_PATH['execution_result'].format(dbms)
            logs_path = OUTPUT_PATH['execution_log'].format(dbms)
        self.results = pd.read_csv(self.results_path, na_filter=False)
        self.logs = pd.read_csv(logs_path, na_filter=False)
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
        self.errors = self.results[self.results['IS_ERROR']]
        return self.errors

    def cluster_error_reasons(self, n_clusters=8, n_init=10, max_iter=300):

        vectorizer = TfidfVectorizer(stop_words='english')
        error_messages = self.results[self.results['IS_ERROR']
                                      == True]['ERROR_MSG']
        X = vectorizer.fit_transform(error_messages)
        kmeans = KMeans(n_clusters=n_clusters, n_init=n_init,
                        max_iter=max_iter)
        kmeans.fit(X)
        self.results.loc[self.results['IS_ERROR'], 'CLUSTER'] = kmeans.labels_
        return kmeans.labels_

    def cluster_result_mismatch(self, n_clusters=8, n_init=10, max_iter=300):

        rm_error_index = self.results['ERROR_MSG'] == 'Result MisMatch'
        rm_errors = self.results[rm_error_index]
        assert len(rm_errors) > 0
        res_similarities = rm_errors.apply(lambda row: compute_similarity(
            row['ACTUAL_RESULT'], row['EXPECTED_RESULT']), axis=1)
        kmeans = KMeans(n_clusters=n_clusters, n_init=n_init,
                        max_iter=max_iter)
        kmeans.fit(res_similarities.values.reshape(-1, 1))
        print(kmeans.labels_)
        self.results.loc[rm_error_index, 'CLUSTER'] = kmeans.labels_ + 100
        return kmeans.labels_

    def dump_errors(self, path: str = 'data/flaky'):
        errors = copy(self.get_error_rows())
        errors['TESTFILE_NAME'] = errors['TESTFILE_PATH'].apply(
            lambda x: convert_testfile_name(x, self.dbms_suite))
        errors.to_csv(f"{path}/{self.dbms_suite}_errors.csv",
                      columns=['TESTFILE_NAME', 'TESTCASE_INDEX', 'CLUSTER'], index=False)

    def dump_results(self, suffix: str = 'clustered'):
        self.results.to_csv(f"{self.results_path.removesuffix('.csv')}{suffix}.csv", index=False)

    def find_dependency_failure(self, row: pd.DataFrame):
        all_results = self.results[self.results['TESTFILE_PATH']
                                   == row.TESTFILE_PATH.values[0]]
        # get the logs of the test case
        logs = self.logs.loc[row.LOGS_INDEX.values[0]].values[0]

        # get the successful dependencies
        succ_ddl_dep = self.find_dependencies(logs, self.DDL)
        succ_dml_dep = self.find_dependencies(logs, self.DML)

        # find the rows in results that has smaller index than the current row
        previous_results = all_results[all_results['TESTCASE_INDEX']
                                       < row.TESTCASE_INDEX.values[0]]
        failed_rows = previous_results[previous_results['IS_ERROR'] == True]

        fail_ddl_dep = self.find_dependencies(
            '\n'.join(failed_rows['SQL'].values), self.DDL)

    def extract_dependency_failure(self, filename: str):
        all_results = self.results[self.results['TESTFILE_PATH'] == filename]
        # add a column to store the dependency
        self.results['DEPENDENCY'] = None
        # all_statements = all_results[all_results['CASE_TYPE'] == 'STATEMENT']
        # iterate the results:
        true_dep = set()
        ddl_dep = set()
        dml_dep = set()
        tcl_dep = set()
        if all_results[all_results['IS_ERROR'] == True].empty:
            return all_results
        for index, row in all_results.iterrows():
            sql = row.SQL
            if len(sql) > 0:
                parsed = sqlparse.parse(sql)[0]
            sql_type = str(parsed.token_first())
            identifiers = set(
                [str(token) for token in parsed.flatten() if token.ttype is sqlparse.tokens.Name])
            if row.IS_ERROR:
                if row.CASE_TYPE == 'Query':
                    # print(row.SQL)
                    # if relation with ddl dependency
                    inter_ddl = identifiers.intersection(ddl_dep)
                    inter_dml = identifiers.intersection(dml_dep)
                    inter_tcl = identifiers.intersection(tcl_dep)
                    inter_true = identifiers.intersection(true_dep)
                    dep = set()
                    if len(inter_ddl) > 0:
                        # update the row of the all_results
                        self.results.loc[index, 'ERROR_REASON'] = 'DEP-DDL'
                        dep = inter_ddl
                    elif len(inter_dml) > 0:
                        self.results.loc[index, 'ERROR_REASON'] = 'DEP-DML'
                        dep = inter_dml
                    elif len(inter_tcl) > 0:
                        self.results.loc[index, 'ERROR_REASON'] = 'DEP-TCL'
                        dep = inter_tcl
                    elif len(inter_true) == 0 and len(inter_ddl) == 0:
                        self.results.loc[index, 'ERROR_REASON'] = 'DEP-EXT'
                    else:
                        print('ERROR: ', row.SQL)
                    if len(dep) > 0:
                        self.results.loc[index, 'DEPENDENCY'] = str(dep)
                if sql_type.upper() in self.DDL:
                    ddl_dep.update(identifiers)
                elif sql_type.upper() in self.DML:
                    dml_dep.update(identifiers)
                # elif sql_type.upper() in self.TCL:
                #     tcl_dep.update(self.find_dependencies(';\n'.join(
                #         all_results[all_results['TESTCASE_INDEX'] < row.TESTCASE_INDEX]), self.DDL + self.DML))
            else:
                if sql_type.upper() in self.DDL:
                    true_dep.update(identifiers)
        print(all_results.info())
        return all_results

    def find_dependencies(self, logs: str, dep_type='CREATE'):
        dependencies = set()
        parsed = sqlparse.parse(logs)
        for sql in parsed:
            if str(sql.token_first()) in dep_type:
                tokens = sql.tokens
                identifiers = [str(token) for token in parsed.flatten(
                ) if token.ttype is sqlparse.tokens.Name]
                # splits the string into a list of substrings at each space.
                # The result is a flattened list of substrings without spaces.
                dependencies.update(identifiers)
        return dependencies
