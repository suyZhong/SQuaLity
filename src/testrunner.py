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

import duckdb
from .utils import *
from .bugdumper import BugDumper
from .config import CONFIG
import signal


class Runner():
    def __init__(self, records: List[Record] = []) -> None:
        self.records = []
        self.setup_records = copy(records)
        self.records_log = []
        self.all_run_stats = {}.fromkeys(Running_Stats, 0)
        self.single_run_stats = {}.fromkeys(Running_Stats, 0)
        self.dump_all = False
        self.bug_dumper = None
        self.dbms_name = ""
        self.cur_time = datetime.now()
        self.end_time = datetime.now()
        self.db = ":memory:"
        self.env = {}
        self.hash_threshold = 8
        self.filter_dict = {}
        self.test_setup()

    def test_setup(self):
        """set up the test environment
        """

    def set_db(self, db_name: str):
        """Set up the Database that this test run should use

        Args:
            db_name (str): The database name.
        """

    def remove_db(self, db_name: str):
        """Remove the temp database

        Args:
            db_name (str): The database name
        """

    def get_records(self, records: List[Record], testfile_index: int, testfile_path: str):
        """get records for the test runner

        Args:
            records (List[Record]): Test record, could be Statement, Query or Control
            testfile_index (int): The index of the test file among all the files
            testfile_path (str): The path of the test file
        """
        self.allright = True
        self.records = records
        self.testfile_index = testfile_index
        self.testfile_path = testfile_path
        self.bug_dumper.get_testfile_data(testfile_index=testfile_index,
                                          testfile_path=testfile_path)

    def filter_records(self):
        """Filter the records that should be executed
        """
        test_name = convert_testfile_name(
            self.testfile_path, DBMS_MAPPING[self.dbms_name])
        if test_name in self.filter_dict:
            test_cases = dict(self.filter_dict[test_name])
            if -1 in test_cases:
                self.single_run_stats['filter_sql'] += len(self.records)
                self.records = []
            else:
                self.records = [
                    record for record in self.records if record.id not in test_cases]
                self.single_run_stats['filter_sql'] += len(test_cases)

    def connect(self, db_name: str):
        """connect to the database instance by the file path or the database name

        Args:
            db_name (str): For embedded DB, it is the filepath. For C/S DB, it is the database name
        """

    def start(self):
        """Start the test runner, preprocess some data
        """
        self.single_run_stats = {}.fromkeys(Running_Stats, 0)
        self.records_log = []
        self.bug_dumper.reset_schema()

        # count the records that should be executed
        self.single_run_stats['total_sql'] += len(
            [record for record in self.records if type(record) == Query or type(record) == Statement])

        # filter the records that are not suitable
        self.filter_records()

    def run(self):
        """The core logic of the test runner
        """

    def close(self):
        """close the current DB connection
        """

    def commit(self):
        """commit the current changes
        """

    def running_summary(self, test_name, running_time):
        """Summary the running stats and output to the stdout

        Args:
            test_name (_type_): Test case name
            running_time (_type_): the running time of the execution
        """
        if self.allright and self.records != []:
            self.single_run_stats['success_file_num'] += 1

        # if the test name is ALL, then the stats should be the all run stats
        if test_name == "ALL":
            stats = self.all_run_stats
        else:
            stats = self.single_run_stats
        print("-------------------------------------------")
        print("Testing DBMS: %s" %
              type(self).__name__.lower().removesuffix("runner"))
        print("Test Case: %s" % test_name)
        print("Success test files: %d" % stats["success_file_num"])
        print("Total SQL:", stats["total_sql"])
        print("Filter SQL:", stats["filter_sql"])
        print("Total executed SQL: ", stats['total_executed_sql'])
        print("Total execution time: %ds" % running_time)
        print("Total SQL statement: ", stats['statement_num'])
        print("Total SQL query: ", stats['query_num'])
        print("Failed SQL statement: ", stats['failed_statement_num'])
        print("Failed SQL query: ", stats['failed_query_num'])
        print("Success SQL query: ", stats['success_query_num'])
        print("Wrong SQL statement: ", stats['wrong_stmt_num'])
        print("Wrong SQL query: ", stats['wrong_query_num'])
        print("-------------------------------------------", flush=True)
        # add the single run stats to the all run stats
        for key in self.single_run_stats:
            self.all_run_stats[key] += self.single_run_stats[key]

    def init_dumper(self, dump_all=False, suite_name=""):
        """init the bug dumper in the test runner

        Args:
            dump_all (bool, optional): Decide whether should dump all the information but not only the bugs. Defaults to False.
        """
        self.dump_all = dump_all
        suffix = "" if self.filter_dict == {} else "_filter"
        suffix = suffix if logging.root.level != logging.DEBUG else suffix + "_debug"
        suffix = suffix if suite_name == "" else f"_{suite_name}{suffix}"
        self.bug_dumper = BugDumper(self.dbms_name, dump_all, suffix)

    def init_filter(self, filter_flag=False):
        """init the filter in the test runner

        Args:
            filter_file (str): The path of the filter file
        """
        if not filter_flag:
            self.filter_dict = {}
            return
        path = SETUP_PATH['filter']
        # read all csv files under the path
        filter_df = pd.concat([pd.read_csv(f"{path}/{file}") for file in os.listdir(
            path) if file.endswith('.csv')], ignore_index=True)
        filter_df[['TESTCASE_INDEX', 'CLUSTER']] = filter_df[[
            'TESTCASE_INDEX', 'CLUSTER']].astype(int)
        # Convert the dataframe to a dict where the key is the first column and the value are the rest columns
        self.filter_dict = filter_df.groupby(filter_df.columns[0]).apply(
            lambda x: x[filter_df.columns[1:]].values.tolist()).to_dict()
        # check if there's -1 in TESTCASE_INDEX, if so, then it means all the test cases should be filtered
        if filter_df['TESTCASE_INDEX'].isin([-1]).any():
            for filename in filter_df[filter_df['TESTCASE_INDEX'] == -1]['TESTFILE_NAME'].values.tolist():
                self.filter_dict[filename].append([-1, -1])

    def not_allright(self):
        self.allright = False

    def dump(self, mode='a'):
        """Dump the results and logs

        Args:
            mode (str, optional): a means add, 'w' means cover write. Defaults to 'a'.
        """
        dump_name = self.dbms_name
        self.bug_dumper.dump_to_csv(dump_name, mode=mode)

    def handle_wrong_query(self, query: Query, result: str, **kwargs):
        self.single_run_stats['wrong_query_num'] += 1
        if 'label' in kwargs:
            logging.error(
                "Query %s does not return expected result. The Expected result is not equal to %s's result", query.sql, query.label)
        else:
            logging.error(
                "Query %s does not return expected result. \nExpected: %s\nActually: %s",
                query.sql, query.result.strip(), result.strip())
            # logging.debug("Expected:\n %s\n Actually:\n %s\n",
            #               query.result.strip(), result.strip())
        self.allright = False
        self.bug_dumper.save_state(self.records_log, query, result, (datetime.now(
        )-self.cur_time).microseconds, is_error=True, msg="Result MisMatch")
        # self.bug_dumper.print_state()

    def handle_wrong_stmt(self, stmt: Statement, status: str, **kwargs):
        self.single_run_stats['wrong_stmt_num'] += 1
        logging.error(
            "Statement %s does not behave as expected", stmt.sql)
        self.allright = False
        if 'err_msg' in kwargs:
            self.bug_dumper.save_state(self.records_log, stmt, str(status), (datetime.now(
            )-self.cur_time).microseconds, is_error=True, msg=kwargs['err_msg'])
        else:
            self.bug_dumper.save_state(self.records_log, stmt, str(
                status), (datetime.now()-self.cur_time).microseconds, is_error=True)


class PyDBCRunner(Runner):
    MAX_RUNTIME = 500
    LARGE_TEST_THRESHOLD = 1000
    MAX_RUNTIME_PERSQL = CONFIG['max_runtime_persql']

    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.allright = True
        # self.db_error = Exception
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.log_level = logging.root.level
        self.exec_time = 0
        self.result_helper = None
        self.testfile_index = 0
        self.testfile_path = ""
        self.labels = {}
        self.db_error = Exception

    def timeout_handler(self, signum, frame):
        raise TimeoutError

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
        self.start()
        class_name = type(self).__name__
        dbms_name = class_name.lower().removesuffix("runner")
        self.exec_time = datetime.now()
        self.labels = {}
        for record in self.records:
            self.cur_time = datetime.now()
            if dbms_name not in record.db:
                continue
            if type(record) == Control:
                action = record.action
                try:
                    self.handle_control(action, record)
                except StopRunnerException:
                    break
            signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(self.MAX_RUNTIME_PERSQL)
            try:
                self._single_run(record)
            except TimeoutError:
                self.end_time = datetime.now()
                logging.warning("Time Exceed - %d" % self.MAX_RUNTIME_PERSQL)
                self.bug_dumper.save_state(self.records_log, record, str(True),
                                           execution_time=(self.end_time-self.cur_time).microseconds, is_error=True, msg="Time Exceed - {}".format(self.MAX_RUNTIME_PERSQL))
                if len(self.records) > self.LARGE_TEST_THRESHOLD:
                    break
            else:
                signal.alarm(0)
            self.end_time = datetime.now()

    def execute_stmt(self, sql):
        pass

    def executemany_stmt(self, sql):
        pass

    def execute_query(self, sql):
        return [()]

    def commit(self):
        pass
    
    def reset_cursor(self):
        pass

    def _single_run(self, record):
        self.single_run_stats['total_executed_sql'] += 1
        if type(record) is Statement:
            self.single_run_stats['statement_num'] += 1
            status = True
            except_msg = None
            try:
                self.execute_stmt(record.sql)
            except self.db_error as e:
                status = False
                self.single_run_stats['failed_statement_num'] += 1
                except_msg = str(e)
                logging.debug(
                    "Statement %s execution error: %s", record.sql, e)
                # self.reset_cursor()
            self.handle_stmt_result(status, record, except_msg)
            # self.commit()
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
                # self.commit()
                self.bug_dumper.save_state(self.records_log, record, str(False), (
                    datetime.now()-self.cur_time).microseconds, is_error=True, msg="{}".format(except_msg))
                self.allright = False
                # self.reset_cursor()
                return
            else:
                self.single_run_stats['success_query_num'] += 1
            # print(results)
            self.handle_query_result(results, record)

    def handle_control(self, action: RunnerAction, record: Record):
        if action == RunnerAction.HALT:
            logging.warning("halt the rest of the test cases")
            self.allright = False
            raise StopRunnerException
        elif action == RunnerAction.ECHO:
            logging.info(record.sql)

    def handle_stmt_result(self, status, record: Statement, err_msg: Exception = None):
        # myDebug("%r %r", status, record.status)
        if status == record.status:
            logging.debug(record.sql + " Success")
            if self.dump_all:
                self.bug_dumper.save_state(self.records_log, record, str(
                    status), (datetime.now()-self.cur_time).microseconds, msg=str(err_msg))
            return True
        else:
            self.handle_wrong_stmt(
                stmt=record, status=status, err_msg=str(err_msg))
            return False

    def handle_query_result(self, results: list, record: Query):
        result_string = ""
        cmp_flag = False
        helper = ResultHelper(results, record)
        if record.sql.startswith("SELECT DISTINCT - - 17 AS col1 FROM tab1, tab0 AS cor0 GROUP BY cor0.col1"):
            my_debug(results)
        if record.label != '' and record.result == '':
            # my_debug(results)
            if record.sort == SortType.ROW_SORT:
                results.sort()
            result_string = helper.hash_results(str(results))
            cmp_flag = True
            if record.label in self.labels:
                cmp_flag = result_string == self.labels[record.label]
            else:
                self.labels[record.label] = result_string

        else:
            if record.res_format == ResultFormat.VALUE_WISE:
                cmp_flag, result_string = helper.value_wise_compare(
                    results, record, self.hash_threshold, record.is_hash)
                # my_debug(result_string)
            elif record.res_format == ResultFormat.ROW_WISE:
                cmp_flag, result_string = helper.row_wise_compare(
                    results, record)
            elif record.res_format == ResultFormat.HASH:
                result_string = '\n'.join(['\n'.join(
                    [str(item) if item != None else 'NULL' for item in row]) for row in results]) + '\n'
                result_string = helper.hash_results(result_string)
                # my_debug(result_string)
                if record.result.find(result_string) != -1:
                    cmp_flag = True
            elif record.res_format == ResultFormat.REGEX:
                cmp_flag, result_string = helper.regex_compare(
                    results, record)
            else:
                logging.warning("Error record result format!")

        if cmp_flag:
            # print("True!")
            my_debug("Query %s Success", record.sql)
            if self.dump_all:
                self.bug_dumper.save_state(
                    self.records_log, record, result_string, (datetime.now()-self.cur_time).microseconds)
        elif record.label != '' and record.result == '':
            self.handle_wrong_query(
                query=record, result=result_string, label=record.label)
        else:
            self.handle_wrong_query(query=record, result=result_string)


class SQLiteRunner(PyDBCRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None
        self.cur = None
        self.txn_status = False
        self.db_error = sqlite3.Error

    def connect(self, db_name):
        logging.info("connect to db %s", db_name)
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()

    def close(self):
        self.con.close()

    def execute_stmt(self, sql):
        if str.upper(sql).startswith('BEGIN'):
            self.txn_status = True
        if str.upper(sql).startswith('COMMIT') or str.upper(sql).startswith('ROLLBACK'):
            self.txn_status = False
        self.cur.execute(sql)
        if not self.txn_status:
            self.con.commit()

    def execute_query(self, sql):
        res = self.cur.execute(sql)
        return res.fetchall()

    def commit(self):
        self.con.commit()


class DuckDBRunner(PyDBCRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None
        self.db_error = duckdb.Error

    def connect(self, db_name):
        logging.info("connect to db %s", db_name)
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
        
    def commit(self):
        self.con.commit()


class CockroachDBRunner(PyDBCRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None
        self.cur = None
        self.db_error = psycopg2.Error

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

        self.con = psycopg2.connect(
            dsn=self.db, options="-c statement_timeout=20s")
        self.con.autocommit = True
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
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.cur = None
        self.con = None
        self.db_error = mysql.connector.Error

    def set_db(self, db_name):
        self.db = db_name

    def remove_db(self, db_name):
        self.cur.fetchall()
        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.commit()
        self.con.close()

    def connect(self, db_name=""):
        logging.info("connect to db %s", db_name)
        self.con = mysql.connector.connect(
            host="localhost", user="root", password="root", port=CONFIG['mysql_port'])
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
        my_debug("BEfore %s : The connection status is %s" % (sql, self.con.is_connected()))
        self.cur.execute(sql)
        self.cur.fetchall()
        my_debug("After: The connection status is %s" % self.con.is_connected())

    def executemany_stmt(self, sql: str):
        sql_list = sql.split(";")
        for stmt in sql_list:
            self.cur.execute(stmt)
        # self.cur.fetchall()
        self.con.commit()
        return

    def commit(self):
        results = self.cur.fetchall()
        if results:
            logging.info("There's unread results, please fetch them first.")
            my_debug(str(results))
        self.con.commit()

    def reset_cursor(self):
        self.cur.close()
        self.cur = self.con.cursor()

class PostgreSQLRunner(CockroachDBRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)

    def set_db(self, db_name):
        self.db = f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/postgres?sslmode=disable"
        self.connect("postgres")
        self.con.autocommit = True

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.execute_stmt("CREATE DATABASE %s" % db_name)
        self.commit()
        self.close()
        dsn = f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/{db_name}?sslmode=disable"
        self.db = dsn

    def remove_db(self, db_name):
        self.db = f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/postgres?sslmode=disable"
        self.connect("postgres")
        self.con.autocommit = True

        self.execute_stmt("DROP DATABASE IF EXISTS %s" % db_name)
        self.commit()
        self.close()


class CLIRunner(Runner):
    def __init__(self, records: List[Record] = []) -> None:
        self.res_delimiter = '------'
        self.echo = ''
        self.cmd = []
        super().__init__(records)
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.cli = None
        self.sql = []
        # shouldn't be too low, otherwise the cli will be very slow and the result for psql would not match
        self.cli_limit = 1000

    def get_env(self):
        """get the environment variable
        """

    def extract_sql(self):
        self.sql = []
        for record in self.records:
            sql = record.sql
            self.sql.append(sql + ';\n')

    def handle_query_result(self, results: str, record: Query):
        """handle the query result
        """
        cmp_flag = False
        helper = ResultHelper(results, record)
        if record.res_format == ResultFormat.VALUE_WISE:
            result_list = [row.split('\t')
                           for row in results.split('\n')] if results else ""
            cmp_flag, results = helper.value_wise_compare(
                result_list, record, self.hash_threshold)
        elif record.res_format == ResultFormat.ROW_WISE:
            cmp_flag, results = helper.row_wise_compare(results, record)
        else:
            logging.error(
                "Result format unsupported for this Runner: %s", record.res_format)
        if cmp_flag:
            # print("True!")
            my_debug("Query %s Success", record.sql)
            if self.dump_all:
                self.bug_dumper.save_state(
                    self.records_log, record, results, (datetime.now()-self.cur_time).microseconds)
        else:
            self.handle_wrong_query(record, results)

    def handle_results(self, output: List[str]):
        for i, result in enumerate(output):
            # TODO add valuewise compare ... here
            record = self.records[i]
            expected_result = record.result
            actually_result, _ = convert_postgres_result(result.strip('\n'))
            actually_status = not bool(re.match(r'^ERROR:', actually_result))

            self.single_run_stats['total_executed_sql'] += 1
            if type(record) == Statement:
                self.single_run_stats['statement_num'] += 1
                if actually_status:
                    self.records_log.append(record)
                if record.status == actually_status:
                    logging.debug("Statement {} Success".format(record.sql))
                    if self.dump_all:
                        self.bug_dumper.save_state(
                            self.records_log, record, str(record.status), 0, msg=actually_result)
                else:
                    self.handle_wrong_stmt(
                        record, actually_status, err_msg=actually_result)
            elif type(record) == Query:
                self.single_run_stats['query_num'] += 1
                self.handle_query_result(actually_result, record)

    def run(self):
        self.start()
        self.extract_sql()
        # split sqls into groups of limit
        if len(self.sql) == 0:
            return
        sql_lists = [self.sql[i:i + self.cli_limit]
                     for i in range(0, len(self.sql), self.cli_limit)]
        whole_output_list = []
        for sql_list in sql_lists:
            self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True, bufsize=2 << 15)
            my_debug("Running %d sqls" % len(sql_list))
            input_string = ""
            for i, sql in enumerate(sql_list):
                # my_debug(sql)
                result = self.records[i].result
                # self.cli.stdin.write(self.echo.format(self.res_delimiter))
                # self.cli.stdin.write(sql)
                # self.cli.stdin.flush()
                input_string += self.echo.format(self.res_delimiter)
                input_string += sql
            output, _ = self.cli.communicate(input=input_string)
            output_list = output.split(self.res_delimiter)[1:]
            if self.cli_limit > len(self.sql):
                my_debug(output)
            assert len(
                output_list) == len(sql_list), "The length of result list should be equal to the commands have executed"
            whole_output_list += output_list
        self.handle_results(whole_output_list)
        self.cli.terminate()


class PSQLRunner(CLIRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.res_delimiter = "*-------------*"
        self.echo = "\\echo {}\n"

    def extract_sql(self):
        self.sql = []
        for record in self.records:
            sql = record.sql
            if re.match(r'^COPY', sql, re.IGNORECASE):
                # the COPY statement in Postgres will try to find file in server side
                # if the backend is docker, the file will be missing
                # So we transform COPY to \copy in psql
                # psql \copy don't support variable substitude so we transform it to command
                if sql.find(":'filename'") >= 0:
                    sql_cmd = [s.replace("\\", "\\\\").replace("'", "\\'") for s in re.sub(
                        r'^(?i)COPY', r'\\copy', sql).split(":'filename'")]
                    self.sql.append("\\set cp_cmd '{}':'filename''{}'\n:cp_cmd\n".format(
                        sql_cmd[0], sql_cmd[1].strip()))
                # it is a copy from stdin, no need to change
                elif type(record) == Statement or type(record) == Query:
                    self.sql.append(sql + ';\n' + record.input_data + '\n')
                continue
            if sql.startswith('\\') or sql.endswith('\\gset'):
                self.sql.append(sql + '\n')
            else:
                self.sql.append(sql + ';\n')

    # TODO make here more elegant

    def get_env(self):
        self.env['PG_LIBDIR'] = subprocess.run(
            ['pg_config', '--pkglibdir'], capture_output=True, encoding='utf-8').stdout.strip()
        self.env['PG_ABS_SRCDIR'] = os.path.abspath(
            TESTCASE_PATH['postgresql'])
        self.env['PG_DLSUFFIX'] = '.so'
        self.env['PG_ABS_BUILDDIR'] = os.path.abspath(OUTPUT_PATH['base'])

    def test_setup(self):
        self.get_env()
        my_debug('setup total {} test cases'.format(len(self.setup_records)))
        if len(self.setup_records) > 0:
            self.records = self.setup_records
            db_name = 'testdb'
            # set the env variable
            for env_name in self.env:
                os.environ[env_name] = self.env[env_name]

            # init a test database
            self.cmd = [
                'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/postgres?sslmode=disable", '-X', '-a', '-q', '-c']
            stmts = [
                "DROP DATABASE IF EXISTS {};\n".format(db_name),
                "CREATE DATABASE {};\n".format(db_name),
            ]
            for stmt in stmts:
                self.execute_stmt(stmt)
            self.cmd = [
                'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/{db_name}?sslmode=disable", '-X', '-q']

            # run the test_setup.sql
            self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True)
            self.extract_sql()
            for i, sql in enumerate(self.sql):
                self.cli.stdin.write(self.echo.format(self.res_delimiter))
                self.cli.stdin.write(sql)
                self.cli.stdin.flush()
            self.cli.stdin.flush()
            output, err = self.cli.communicate()
            my_debug(output)
            my_debug(err)
            self.cli.terminate()

    def set_db(self, db_name: str):
        # if the test suites have some setup records, then don't set up db when each test
        if len(self.setup_records) > 0:
            return
        my_debug('set up db {}'.format(db_name))
        self.cmd = [
            'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/postgres?sslmode=disable", '-X', '-a', '-q', '-c']
        stmts = [
            "DROP DATABASE IF EXISTS {};\n".format(db_name),
            "CREATE DATABASE {};\n".format(db_name),
        ]
        for stmt in stmts:
            self.execute_stmt(stmt)
        self.cmd = [
            'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/{db_name}?sslmode=disable", '-X', '-q']

    def remove_db(self, db_name: str):
        # return
        if len(self.records) >= 0:
            return
        self.cmd = [
            'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/postgres?sslmode=disable", '-X', '-a', '-q', '-c']
        self.execute_stmt("DROP DATABASE IF EXISTS {}".format(db_name))
        self.cmd = ['psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/{db_name}?sslmode=disable".format(
            db_name), '-X', '-q']

    def execute_stmt(self, sql):
        cmd = self.cmd + [sql]
        psql_proc = subprocess.run(
            cmd, capture_output=True, encoding='utf-8')
        if psql_proc.stderr:
            # my_debug('execute failed {}'.format(psql_proc.stderr) )
            if str(psql_proc.stderr).find('ERROR:') > 0:
                raise DBEngineExcetion(psql_proc.stderr)

    def execute_query(self, sql: str):
        psql_proc = subprocess.run(
            self.cmd + [sql], capture_output=True, universal_newlines=True, encoding='utf-8')
        if psql_proc.stderr:
            raise DBEngineExcetion(psql_proc.stderr)
        else:
            result_string, _ = convert_postgres_result(
                "\n".join(psql_proc.stdout.split('\n')[len(sql.split('\n')): -1]))
            return result_string
