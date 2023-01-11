import os
import sqlite3
import psycopg2
import logging
import math
import subprocess
import mysql.connector
from typing import List
from datetime import datetime
from copy import copy
from tqdm import tqdm

import duckdb
from .utils import *
from .bugdumper import BugDumper


class Runner():
    def __init__(self) -> None:
        self.all_run_stats = {}.fromkeys(Running_Stats, 0)
        self.single_run_stats = {}.fromkeys(Running_Stats, 0)
        self.dump_all = False
        self.bug_dumper = None
        self.dbms_name = ""

    def set_db(self, db_name: str):
        """Set up the Database that this test run should use

        Args:
            db_name (str): The database name.
        """
        pass

    def get_records(self, records: List[Record], testfile_index: int, testfile_path: str):
        """get records for the test runner

        Args:
            records (List[Record]): Test record, could be Statement, Query or Control
            testfile_index (int): The index of the test file among all the files
            testfile_path (str): The path of the test file
        """
        pass

    def connect(self, db_name: str):
        """connect to the database instance by the file path or the database name

        Args:
            db_name (str): For embedded DB, it is the filepath. For C/S DB, it is the database name
        """
        pass

    def run(self):
        """The core logic of the test runner
        """
        pass

    def close(self):
        """close the current DB connection
        """
        pass

    def commit(self):
        """commit the current changes
        """
        pass

    def running_summary(self, test_name, running_time):
        """Summary the running stats and output to the stdout

        Args:
            test_name (_type_): Test case name
            running_time (_type_): the running time of the execution
        """
        if test_name == "ALL":
            stats = self.all_run_stats

        else:
            stats = self.single_run_stats
        print("-------------------------------------------")
        print("Testing DBMS: %s" %
              type(self).__name__.lower().removesuffix("runner"))
        print("Test Case: %s" % test_name)
        print("Total execute SQL: ", stats['total_sql'])
        print("Total execution time: %ds" % running_time)
        print("Total SQL statement: ", stats['statement_num'])
        print("Total SQL query: ", stats['query_num'])
        print("Failed SQL statement: ", stats['failed_statement_num'])
        print("Failed SQL query: ", stats['failed_query_num'])
        print("Success SQL query: ", stats['success_query_num'])
        print("Wrong SQL statement: ", stats['wrong_stmt_num'])
        print("Wrong SQL query: ", stats['wrong_query_num'])
        print("-------------------------------------------", flush=True)

    def init_dumper(self, dump_all=False):
        """init the bug dumper in the test runner

        Args:
            dump_all (bool, optional): Decide whether should dump all the information but not only the bugs. Defaults to False.
        """
        self.dump_all = dump_all
        self.bug_dumper = BugDumper(self.dbms_name, dump_all)

    def dump(self, mode='a'):
        """Dump the results and logs

        Args:
            mode (str, optional): a means add, 'w' means cover write. Defaults to 'a'.
        """
        self.bug_dumper.dump_to_csv(self.dbms_name, mode=mode)


class PyDBCRunner(Runner):
    MAX_RUNTIME = 500
    MAX_RUNTIME_PERSQL = 10

    def __init__(self) -> None:
        super().__init__()
        self.records = []
        self.hash_threshold = 8
        self.allright = True
        self.db_error = Exception
        self.db = ":memory:"
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.records_log = []
        self.log_level = logging.root.level
        self.exec_time = 0
        self.result_helper = None
        self.cur_time = datetime.now()
        self.end_time = datetime.now()
        self.testfile_index = 0
        self.testfile_path = ""
        self.labels = {}

    def set_db(self, db_name: str):
        if not db_name.startswith(":memory:"):
            os.system('rm -f %s' % db_name)
        self.db = db_name

    def remove_db(self, db_name: str):
        if self.allright:
            logging.info("Pass all test cases!")
            try:
                os.remove(self.db)
            except:
                logging.error("No such file or directory: %s", self.db)

    def run(self):
        class_name = type(self).__name__
        dbms_name = class_name.lower().removesuffix("runner")
        # TODO make here more elegant
        if dbms_name == 'psql':
            dbms_name = 'postgresql'
        self.single_run_stats = {}.fromkeys(Running_Stats, 0)
        self.records_log = []
        self.exec_time = datetime.now()
        self.bug_dumper.reset_schema()
        self.labels = {}
        for record in self.records:
            self.cur_time = datetime.now()
            # if (self.cur_time - self.exec_time).seconds > self.MAX_RUNTIME:
            #     logging.warning("Time exceed for this testfile", )
            #     break
            if dbms_name not in record.db:
                continue
            if type(record) == Control:
                action = record.action
                try:
                    self.handle_control(action, record)
                except StopRunnerException:
                    break

            self._single_run(record)
            self.end_time = datetime.now()
            exec_time = (self.end_time-self.cur_time).seconds

            # If some SQL query too slow
            if exec_time > self.MAX_RUNTIME_PERSQL:
                logging.warning("Time Exceed - %d" % exec_time)
                self.bug_dumper.save_state(self.records_log, record, "Time Exceed - %d" % exec_time,
                                           execution_time=(self.end_time-self.cur_time).microseconds, is_error=True)
                break
        for key in self.single_run_stats:
            self.all_run_stats[key] += self.single_run_stats[key]

    def get_records(self, records: List[Record], testfile_index: int, testfile_path: str):
        self.allright = True
        self.records = records
        self.testfile_index = testfile_index
        self.testfile_path = testfile_path
        self.bug_dumper.get_testfile_data(testfile_index=testfile_index,
                                          testfile_path=testfile_path)

    def execute_stmt(self, sql):
        pass

    def executemany_stmt(self, sql):
        pass

    def execute_query(self, sql):
        return [()]

    def commit(self):
        pass

    def _single_run(self, record):
        self.single_run_stats['total_sql'] += 1
        if type(record) is Statement:
            self.single_run_stats['statement_num'] += 1
            status = True
            try:
                self.execute_stmt(record.sql)
            except self.db_error as except_msg:
                status = False
                self.single_run_stats['failed_statement_num'] += 1
                logging.debug(
                    "Statement %s execution error: %s", record.sql, except_msg)
            self.handle_stmt_result(status, record)
            self.commit()
            if status:
                self.records_log.append(record)
        elif type(record) is Query:
            self.single_run_stats['query_num'] += 1
            results = []
            try:
                results = self.execute_query(record.sql)
            except self.db_error as except_msg:
                self.single_run_stats['failed_query_num'] += 1
                logging.debug("Query %s execution error: %s",
                              record.sql, except_msg)
                self.commit()
                self.bug_dumper.save_state(self.records_log, record, "Execution Failed: %s" % except_msg, (
                    datetime.now()-self.cur_time).microseconds, is_error=True)
                return
            else:
                self.single_run_stats['success_query_num'] += 1
            # print(results)
            if type(results) == list:
                self.handle_query_result_list(results, record)
            else:
                self.handle_query_result_string(results, record)

    def not_allright(self):
        self.allright = False

    def handle_control(self, action: RunnerAction, record: Record):
        if action == RunnerAction.HALT:
            logging.warning("halt the rest of the test cases")
            self.allright = False
            raise StopRunnerException
        elif action == RunnerAction.ECHO:
            logging.info(record.sql)

    def handle_stmt_result(self, status, record: Statement):
        # myDebug("%r %r", status, record.status)
        if status == record.status:
            logging.debug(record.sql + " Success")
            if self.dump_all:
                self.bug_dumper.save_state(self.records_log, record, str(
                    status), (datetime.now()-self.cur_time).microseconds)
            return True
        else:
            self.single_run_stats['wrong_stmt_num'] += 1
            logging.error(
                "Statement %s does not behave as expected", record.sql)
            self.allright = False
            self.bug_dumper.save_state(self.records_log, record, str(
                status), (datetime.now()-self.cur_time).microseconds, is_error=True)
            return False

    def handle_query_result_string(self, results: str, record: Query):
        result_string = results
        expected_result = record.result
        cmp_flag = False
        if results == expected_result:
            cmp_flag = True
        else:
            cmp_flag = False

        if cmp_flag:
            my_debug("Query %s Success", record.sql)
            if self.dump_all:
                self.bug_dumper.save_state(
                    self.records_log, record, result_string, (datetime.now()-self.cur_time).microseconds)
        else:
            self.single_run_stats['wrong_query_num'] += 1
            logging.error(
                "Query %s does not return expected result. \nExpected: %s\nActually: %s",
                record.sql, record.result.strip(), result_string.strip())
            logging.debug("Expected:\n %s\n Actually:\n %s\nReturn Table:\n %s\n",
                          record.result.strip(), result_string.strip(), results)
            self.allright = False
            self.bug_dumper.save_state(self.records_log, record, result_string, (datetime.now(
            )-self.cur_time).microseconds, is_error=True)

    def handle_query_result_list(self, results: list, record: Query):
        result_string = ""
        cmp_flag = False
        helper = ResultHelper(results, record)
        if record.label != '' and record.result == '':
            # my_debug(results)
            result_string = helper.hash_results(str(results))
            cmp_flag = True
            if record.label in self.labels:
                cmp_flag = result_string == self.labels[record.label]
            else:
                self.labels[record.label] = result_string

        else:
            if record.res_format == ResultFormat.VALUE_WISE:
                cmp_flag, result_string = helper.value_wise_compare(
                    results, record, self.hash_threshold)
            elif record.sort != SortType.NO_SORT:
                # If it has sort type, then it must be value wise
                hash_threshold = record.res_format == ResultFormat.HASH
                cmp_flag, result_string = helper.value_wise_compare(
                    results, record, hash_threshold)
            # Currently it is only for DuckDB records
            elif record.res_format == ResultFormat.ROW_WISE:
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
            elif record.res_format == ResultFormat.HASH:
                result_string = '\n'.join(['\n'.join(
                    [str(item) if item != None else 'NULL' for item in row]) for row in results]) + '\n'
                result_string = helper.hash_results(result_string)
                # my_debug(result_string)
            else:
                logging.warning("Error record result format!")

        if cmp_flag:
            # print("True!")
            my_debug("Query %s Success", record.sql)
            if self.dump_all:
                self.bug_dumper.save_state(
                    self.records_log, record, result_string, (datetime.now()-self.cur_time).microseconds)
        elif record.label != '':
            self.single_run_stats['wrong_query_num'] += 1
            logging.error(
                "Query %s does not return expected result. The Expected result is not equal to %s's result", record.sql, record.label)
            self.allright = False
            self.bug_dumper.save_state(self.records_log, record, result_string, (datetime.now(
            )-self.cur_time).microseconds, is_error=True)
        else:
            self.single_run_stats['wrong_query_num'] += 1
            logging.error(
                "Query %s does not return expected result. \nExpected: %s\nActually: %s",
                record.sql, record.result.strip(), result_string.strip())
            logging.debug("Expected:\n %s\n Actually:\n %s\nReturn Table:\n %s\n",
                          record.result.strip(), result_string.strip(), results)
            self.allright = False
            self.bug_dumper.save_state(self.records_log, record, result_string, (datetime.now(
            )-self.cur_time).microseconds, is_error=True)
            # self.bug_dumper.print_state()


class SQLiteRunner(PyDBCRunner):
    def __init__(self) -> None:
        super().__init__()
        self.con = None
        self.cur = None

    def connect(self, file_path):
        logging.info("connect to db %s", file_path)
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()

    def close(self):
        self.con.close()

    def execute_stmt(self, sql):
        self.cur.execute(sql)

    def execute_query(self, sql):
        res = self.cur.execute(sql)
        return res.fetchall()

    def commit(self):
        self.con.commit()


class DuckDBRunner(PyDBCRunner):
    def __init__(self) -> None:
        super().__init__()
        self.con = None
        # self.db_error = (duckdb.ProgrammingError, duckdb.DataError, duckdb.IOException,duckdb.PermissionException)

    def connect(self, file_path):
        logging.info("connect to db %s", file_path)
        self.con = duckdb.connect(database=self.db)

    def close(self):
        self.con.close()

    def execute_query(self, sql):
        self.con.execute(sql)
        return self.con.fetchall()

    def execute_stmt(self, sql):
        self.con.execute(sql)
        self.con.fetchall()

    def executemany_stmt(self, sql):
        self.con.executemany(sql)
        # self.con.fetchall()


class CockroachDBRunner(PyDBCRunner):
    def __init__(self) -> None:
        super().__init__()
        self.con = None
        self.cur = None
        # self.db_error(psycopg2.ProgrammingError)

    def set_db(self, db_name):
        self.db = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
        self.connect("defaultdb")

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.execute_stmt("CREATE DATABASE %s" % db_name)
        self.execute_stmt("USE %s" % db_name)
        self.commit()
        self.close()
        dsn = "postgresql://root@localhost:26257/%s?sslmode=disable" % db_name
        self.db = dsn

    def remove_db(self, db_name):
        self.db = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
        self.connect("defaultdb")
        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.commit()
        self.close()

    def connect(self, db_name):
        logging.info("connect to db %s", db_name)

        self.con = psycopg2.connect(dsn=self.db)
        self.cur = self.con.cursor()

    def close(self):
        self.con.close()

    def execute_query(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()

    def execute_stmt(self, sql):
        self.cur.execute(sql)

    def commit(self):
        self.con.commit()


class MySQLRunner(PyDBCRunner):
    def __init__(self) -> None:
        super().__init__()
        self.cur = None
        self.con = None

    def set_db(self, db_name):
        self.db = db_name

    def remove_db(self, db_name):
        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.commit()
        self.con.close()

    def connect(self, db_name=""):
        logging.info("connect to db %s", db_name)
        self.con = mysql.connector.connect(
            host="localhost", user="root", password="root", port=3307)
        self.cur = self.con.cursor()

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.execute_stmt("CREATE DATABASE %s" % db_name)
        self.execute_stmt("USE %s" % db_name)
        self.commit()

    def close(self):
        # self.con.close()
        pass

    def execute_query(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()

    def execute_stmt(self, sql):
        self.cur.execute(sql)

    def executemany_stmt(self, sql: str):
        sql_list = sql.split(";")
        for stmt in sql_list:
            self.cur.execute(stmt)
        # self.cur.fetchall()
        self.con.commit()
        return

    def commit(self):
        self.con.commit()


class PostgreSQLRunner(CockroachDBRunner):
    def __init__(self) -> None:
        super().__init__()

    def set_db(self, db_name):
        self.db = "postgresql://postgres:root@localhost:5432/postgres?sslmode=disable"
        self.connect("postgres")
        self.con.autocommit = True

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.execute_stmt("CREATE DATABASE %s" % db_name)
        self.commit()
        self.close()
        dsn = "postgresql://postgres:root@localhost:5432/%s?sslmode=disable" % db_name
        self.db = dsn

    def remove_db(self, db_name):
        self.db = "postgresql://postgres:root@localhost:5432/postgres?sslmode=disable"
        self.connect("postgres")
        self.con.autocommit = True

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.commit()
        self.close()


class CLIRunner(Runner):
    def __init__(self) -> None:
        super().__init__()
        self.cli = None
        self.sql = []
        self.cmd = []
        self.res_delimiter = '------'

    def set_db(self, db_name: str):
        return super().set_db(db_name)

    def extract_sql(self):
        self.sql = []
        for record in self.records:
            self.sql.append(record.sql + ';\n')

    def handle_results(self, output: str):
        pass

    def run(self):
        self.extract_sql()
        self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True)
        for i, sql in enumerate(self.sql):
            self.cli.stdin.write(self.res_delimiter)
            self.cli.stdin.write(sql)
            self.cli.stdin.flush()
        output, _ = self.cli.communicate()
        self.handle_results(output)

    def debug(self):
        dbms_name = 'tempdb'
        psql_cmd = [
            'psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(dbms_name), '-X', '-a', '-q']
        psql_cli = subprocess.Popen(['psql', 'postgresql://postgres:root@localhost:5432/?sslmode=disable', '-X', '-q'],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True)

        queries = ['\d\n', 'CREATE TABLE BIT_TABLE(b BIT(11));\n', "INSERT INTO BIT_TABLE VALUES (B'10');\n",
                   "INSERT INTO BIT_TABLE VALUES (B'00000000000');\n", '\d\n', 'CREATE TABLE large_table (id INT, data TEXT);\n', '\d\n']
        # for i in range(1,10000):
        #     queries.append("\echo ----------")

        for i, query in enumerate(tqdm(queries)):
            # print(query)
            psql_cli.stdin.write('\echo {}---------------\n'.format(i))
            psql_cli.stdin.write(query)
            psql_cli.stdin.flush()
            # print(psql_cli.stdout.read())
            # psql_proc = subprocess.run(psql_cmd + [query], capture_output=True)
        output, err = psql_cli.communicate()
        print('output len:\n', output)
        print('error:', err)
        psql_cli.terminate()


class PSQLRunner(CLIRunner):
    def __init__(self) -> None:
        super().__init__()
        self.base_url = "postgresql://postgres:root@localhost:5432/{}?sslmode=disable"
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            'postgres'), '-X', '-a', '-q', '-c']

    def set_db(self, db_name: str):
        my_debug('set up db {}'.format(db_name))
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            'postgres'), '-X', '-a', '-q', '-c']
        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.execute_stmt("CREATE DATABASE %s" % db_name)
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            db_name), '-X', '-a', '-q', '-c']

    def remove_db(self, db_name: str):
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            'postgres'), '-X', '-a', '-q', '-c']
        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)

    def commit(self):
        pass

    def close(self):
        pass

    def connect(self, db_name):
        pass

    def execute_stmt(self, sql):
        psql_proc = subprocess.run(
            self.cmd + [sql], capture_output=True, encoding='utf-8')
        if psql_proc.stderr:
            # my_debug('execute failed {}'.format(psql_proc.stderr) )
            raise DBEngineExcetion(psql_proc.stderr)

    def execute_query(self, sql: str):
        psql_proc = subprocess.run(
            self.cmd + [sql], capture_output=True, universal_newlines=True, encoding='utf-8')
        if psql_proc.stderr:
            raise DBEngineExcetion(psql_proc.stderr)
        else:
            result_string = convert_postgres_result(
                "\n".join(psql_proc.stdout.split('\n')[len(sql.split('\n')): -1]))
            return result_string
