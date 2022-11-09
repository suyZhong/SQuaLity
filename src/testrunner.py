import os
import sqlite3
import psycopg2
import hashlib
import logging
import mysql.connector
from typing import List
from datetime import datetime

import pandas as pd

import duckdb
from .utils import *
from .bugdumper import BugDumper


class Runner():
    MAX_RUNTIME = 500
    MAX_RUNTIME_PERSQL = 10

    def __init__(self) -> None:
        self.records = []
        self.hash_threshold = 8
        self.allright = True
        self.all_run_stats = {}.fromkeys(Running_Stats, 0)
        self.single_run_stats = {}.fromkeys(Running_Stats, 0)
        self.db_error = Exception
        self.db = ":memory:"
        self.dbms_name = type(self).__name__.lower().removesuffix("runner")
        self.records_log = []
        self.log_level = logging.root.level
        self.exec_time = 0
        self.dump_all = False
        self.bug_dumper = None
        self.cur_time = datetime.now()
        self.end_time = datetime.now()
        self.testfile_index = 0
        self.testfile_path = ""
        self.labels = {}

    def init_dumper(self, dump_all=False):
        self.dump_all = dump_all
        self.bug_dumper = BugDumper(self.dbms_name, dump_all)

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

    def dump(self, mode='a'):
        self.bug_dumper.dump_to_csv(self.dbms_name, mode=mode)

    def run(self):
        class_name = type(self).__name__
        dbms_name = class_name.lower().removesuffix("runner")
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
                    self.handle_control(action)
                except StopRunnerException:
                    break

            self._single_run(record)
            self.end_time = datetime.now()
            exec_time = (self.end_time-self.cur_time).seconds
            if exec_time > self.MAX_RUNTIME_PERSQL:
                logging.warning("Time Exceed - %d" % exec_time)
                self.bug_dumper.save_state(self.records_log, record, "Time Exceed - %d" % exec_time,
                                           execution_time=(self.end_time-self.cur_time).microseconds, is_error=True)
                break
        for key in self.single_run_stats:
            self.all_run_stats[key] += self.single_run_stats[key]

    def running_summary(self, test_name, running_time):
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
            self.handle_query_result(results, record)

    def not_allright(self):
        self.allright = False

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
        return str(item)

    def _format_results(self, results, datatype: str):
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

    def _hash_results(self, results: str):
        """hash the result string

        Args:
            results (str): a string of results

        Returns:
            _type_: hash value string
        """
        return hashlib.md5(results.encode(encoding='utf-8')).hexdigest()

    def _sort_result(self, results, sort_type=SortType.ROW_SORT):
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

    def handle_control(self, action: RunnerAction):
        if action == RunnerAction.HALT:
            logging.warning("halt the rest of the test cases")
            self.allright = False
            raise StopRunnerException

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

    def handle_query_result(self, results: list, record: Query):
        result_string = ""
        cmp_flag = False
        if record.label != '':
            result_string = self._hash_results(str(results))
            if record.label in self.labels:
                cmp_flag = result_string == self.labels[record.label]
            else:
                self.labels[record.label] = result_string
                
        else:
            if record.res_format == ResultFormat.VALUE_WISE:
                # get result length
                # myDebug(results)
                if results:
                    result_len = len(results) * len(results[0])
                    # Format the result by the query command para
                    results_fmt = self._format_results(
                        results=results, datatype=record.data_type)
                    # sort result and output flat list
                    # myDebug(results_fmt)
                    result_string = self._sort_result(
                        results_fmt, sort_type=record.sort)
                else:
                    result_len = 0
                if result_len > self.hash_threshold:
                    result_string = self._hash_results(result_string)
                    result_string = str(result_len) + \
                        " values hashing to " + result_string
                cmp_flag = result_string.strip() == record.result.strip()
            elif record.res_format == ResultFormat.ROW_WISE:
                expected_result_list = record.result.strip().split('\n') if record.result else []
                expected_result_list.sort()
                actually_result_list = ["\t".join([str(item) if item != None else 'NULL' for item in row])
                                   for row in results]
                actually_result_list.sort()
                cmp_flag = expected_result_list == actually_result_list
                result_string = '\n'.join(actually_result_list)
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


class SQLiteRunner(Runner):
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


class DuckDBRunner(Runner):
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


class CockroachDBRunner(Runner):
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

    def connect(self, file_name):
        logging.info("connect to db %s", file_name)

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


class MySQLRunner(Runner):
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
            host="localhost", user="root", password="root", port=3306)
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
