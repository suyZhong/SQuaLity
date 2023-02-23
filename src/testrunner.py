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
        if self.allright:
            self.single_run_stats['success_file_num'] += 1

        # if the test name is ALL, then the stats should be the all run stats
        if test_name == "ALL":
            stats = self.all_run_stats
        else:
            # add the single run stats to the all run stats
            for key in self.single_run_stats:
                self.all_run_stats[key] += self.single_run_stats[key]
            stats = self.single_run_stats
        print("-------------------------------------------")
        print("Testing DBMS: %s" %
              type(self).__name__.lower().removesuffix("runner"))
        print("Test Case: %s" % test_name)
        print("Success test files: %d" % stats["success_file_num"])
        print("Total SQL:", stats["total_sql"])
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

    def init_dumper(self, dump_all=False):
        """init the bug dumper in the test runner

        Args:
            dump_all (bool, optional): Decide whether should dump all the information but not only the bugs. Defaults to False.
        """
        self.dump_all = dump_all
        self.bug_dumper = BugDumper(self.dbms_name, dump_all)

    def not_allright(self):
        self.allright = False

    def dump(self, mode='a'):
        """Dump the results and logs

        Args:
            mode (str, optional): a means add, 'w' means cover write. Defaults to 'a'.
        """
        self.bug_dumper.dump_to_csv(self.dbms_name, mode=mode)

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
        )-self.cur_time).microseconds, is_error=True)
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
    MAX_RUNTIME_PERSQL = 20

    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.hash_threshold = 8
        self.allright = True
        self.db_error = Exception
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.log_level = logging.root.level
        self.exec_time = 0
        self.result_helper = None
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
        self.start()
        class_name = type(self).__name__
        dbms_name = class_name.lower().removesuffix("runner")
        self.exec_time = datetime.now()
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
                self.bug_dumper.save_state(self.records_log, record, str(True),
                                           execution_time=(self.end_time-self.cur_time).microseconds, is_error=True, msg="Time Exceed - {}".format(exec_time))
                #break

    def execute_stmt(self, sql):
        pass

    def executemany_stmt(self, sql):
        pass

    def execute_query(self, sql):
        return [()]

    def commit(self):
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
            self.handle_stmt_result(status, record, except_msg)
            self.commit()
            if status == record.status:
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
                self.bug_dumper.save_state(self.records_log, record, str(False), (
                        datetime.now() - self.cur_time).microseconds, is_error=True, msg="Execution Failed: {}".format(except_msg))
                self.allright = False
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
                    results, record, self.hash_threshold, record.is_hash)
            elif record.res_format == ResultFormat.ROW_WISE:
                cmp_flag, result_string = helper.row_wise_compare(
                    results, record)
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
                    self.records_log, record, result_string, (datetime.now() - self.cur_time).microseconds)
        elif record.label != '':
            self.handle_wrong_query(
                query=record, result=result_string, label=record.label)
        else:
            self.handle_wrong_query(query=record, result=result_string)


class SQLiteRunner(PyDBCRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None
        self.cur = None

    def connect(self, db_name):
        logging.info("connect to db %s", db_name)
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
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None
        # self.db_error = (duckdb.ProgrammingError, duckdb.DataError, duckdb.IOException,duckdb.PermissionException)

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


class CockroachDBRunner(PyDBCRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
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
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
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
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)

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
    def __init__(self, records: List[Record] = []) -> None:
        self.res_delimiter = '------'
        self.echo = ''
        self.cmd = []
        super().__init__(records)
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.cli = None
        self.sql = []

    def get_env(self):
        """get the environment variable
        """

    def extract_sql(self):
        self.sql = []
        for record in self.records:
            sql = record.sql
            self.sql.append(sql + ';\n')

    def handle_results(self, output: List[str]):
        for i, result in enumerate(output):
            record = self.records[i]
            expected_result = record.result
            actually_result = convert_postgres_result(result.strip('\n'))
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
                if expected_result == actually_result:
                    my_debug("Query {} Success".format(record.sql))
                    if self.dump_all:
                        self.bug_dumper.save_state(
                            self.records_log, record, actually_result, 0)
                else:
                    # TODO we should know if the query success or failed.
                    self.handle_wrong_query(record, actually_result)

    def run(self):
        self.start()
        self.extract_sql()
        self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True)
        i = 0
        for i, sql in enumerate(self.sql):
            self.cli.stdin.write(self.echo.format(self.res_delimiter))
            self.cli.stdin.write(sql)
            result = self.records[i].result
            self.cli.stdin.flush()
        output, _ = self.cli.communicate()
        output_list = output.split(self.res_delimiter)[1:]
        my_debug(output)
        assert len(
            output_list) == i + 1, "The length of result list should be equal to the commands have executed"
        self.handle_results(output_list)
        self.cli.terminate()


class PSQLRunner(CLIRunner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.base_url = "postgresql://postgres:root@localhost:5432/{}?sslmode=disable"
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
                    sql_cmd = [s.replace("\\", "\\\\").replace("'", "\\'") for s in
                               re.sub(
                        r'^(?i)COPY', r'\\copy', sql).split(":'filename'")]
                    self.sql.append(
                        "\\set cp_cmd '{}':'filename''{}'\n:cp_cmd\n".format(
                        sql_cmd[0], sql_cmd[1].strip()))
                    # it is a copy from stdin, no need to change
                elif type(record) == Statement or type(record) == Query:
                    self.sql.append(sql + ';\n' + record.input_data + '\n')
                continue
            if sql.startswith('\\'):
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
            self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
                'postgres'), '-X', '-a', '-q', '-c']
            stmts = [
                "DROP DATABASE IF EXISTS {};\n".format(db_name),
                "CREATE DATABASE {};\n".format(db_name),
            ]
            for stmt in stmts:
                self.execute_stmt(stmt)
            self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
                db_name), '-X', '-q']

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
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            'postgres'), '-X', '-a', '-q', '-c']
        stmts = [
            "DROP DATABASE IF EXISTS {};\n".format(db_name),
            "CREATE DATABASE {};\n".format(db_name),
        ]
        for stmt in stmts:
            self.execute_stmt(stmt)
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            db_name), '-X', '-q']

    def remove_db(self, db_name: str):
        # return
        if len(self.records) > 0:
            return
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
            'postgres'), '-X', '-a', '-q', '-c']
        self.execute_stmt("DROP DATABASE IF EXISTS {}".format(db_name))
        self.cmd = ['psql', 'postgresql://postgres:root@localhost:5432/{}?sslmode=disable'.format(
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
            result_string = convert_postgres_result(
                "\n".join(psql_proc.stdout.split('\n')[len(sql.split('\n')): -1]))
            return result_string
