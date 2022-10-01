import os
from typing import List
from .utils import *
import hashlib
import logging

import sqlite3
import duckdb

class Runner():
    def __init__(self, records:List[Record]) -> None:
        self.records = records
        self.hash_threshold = 8
        self.allright = True
        self.total_sql = 0
        self.failed_statement_num = 0
        self.failed_query_num = 0
        self.wrong_query_num = 0
        self.statement_num = 0
        self.query_num = 0
        
    def run(self):
        self.total_sql = 0
        self.failed_statement_num = 0
        self.failed_query_num = 0
        self.wrong_query_num = 0
        self.statement_num = 0
        self.query_num = 0
        for record in self.records:
            self._single_run(record)
    
    def running_summary(self, test_name):
        print("-------------------------------------------")
        print("%s Result", test_name)
        print("Total execute SQL: ", self.total_sql)
        print("Total SQL statement: ", self.statement_num)
        print("Total SQL query: ", self.query_num)
        print("Failed SQL statement: ", self.failed_statement_num)
        print("Failed SQL query: ", self.failed_query_num)
        print("Wrong SQL query: ", self.wrong_query_num)
        print("-------------------------------------------")
    
    def get_records(self, records:List[Record]):
        self.allright = True
        self.records = records
        
    def _single_run(record):
        pass
        
    def debug_run(self, iter=1):
        for i, record in enumerate(self.records):
            if i > iter:
                break
            print(record.sql)
            self._single_run(record)
    
    def not_allright(self):
        self.allright = False
    
    def _hash_results(self,results:str):
        """hash the result string

        Args:
            results (str): a string of results

        Returns:
            _type_: hash value string
        """        
        return hashlib.md5(results.encode(encoding='utf-8')).hexdigest()
    
    def _sort_result(self, results, sort_type=SortType.RowSort):
        """sort the result (rows of the results)

        Args:
            results (A list, each item is a tuple)): results list
            sort_type (sort type, optional): . Defaults to SortType.RowSort.

        Returns:
            str: A str of results
        """        
        result_flat = []
        if sort_type == SortType.RowSort:
            results = [list(map(str,row)) for row in results]
            results.sort()
            for row in results:
                for item in row:
                    result_flat.append(item+'\n')
        elif sort_type == SortType.ValueSort:
            for row in results:
                for item in row:
                    result_flat.append(str(item)+'\n')
            result_flat.sort()
        else:
            for row in results:
                for item in row:
                    result_flat.append(str(item)+'\n')
        return ''.join(result_flat)
    
    def _replace_None(self, result_string:str):
        return result_string.replace("None", "NULL")
    
    def handle_stmt_result(self, status, record:Statement):
        # myDebug("%r %r", status, record.status)
        if status == record.status:
            logging.debug(record.sql + " Success")
        else:
            logging.error("Statement %s does not behave as expected", record.sql)
            self.allright = False
    
    def handle_query_result(self, results:list, record:Query):
        result_string = ""
            # get result length
        if results:
            result_len = len(results) * len(results[0])
            # sort result and output flat list
            result_string = self._sort_result(results, sort_type=record.sort)
        else:
            result_len = 0
        result_string = self._replace_None(result_string)
        
        if result_len > self.hash_threshold:
            result_string = self._hash_results(result_string)
            result_string = str(result_len) + " values hashing to " + result_string
        
        
        if result_string.strip() == record.result.strip():
            # print("True!")
            myDebug("Query %s Success", record.sql)
            pass
        else:
            self.wrong_query_num += 1
            logging.error("Query %s does not return expected result", record.sql)
            logging.debug("Expected:\n %s\n Actually:\n %s\nReturn Table:\n %s\n",
                          record.result.strip(), result_string.strip(), results)
            # print(record.sql)
            # print("False")
            # print(results, result_string)
            # print(record.result)
            self.allright = False
        

class SQLiteRunner(Runner):
    def __init__(self, records: List[Record]=[]) -> None:
        super().__init__(records)
        self.con = None
        self.cur = None
        
    
    def connect(self, file_path):
        logging.info("connect to db %s", file_path)
        self.con = sqlite3.connect(file_path)
        self.cur = self.con.cursor()
        
    def close(self):
        self.con.close()
    
    # TODO make it go to super class (Runner) 
    def _single_run(self, record:Record):
        # print(record.sql)
        if 'sqlite' not in record.db:
            return
        self.total_sql += 1
        if type(record) is Statement:
            status = True
            self.statement_num += 1
            try:
                res = self.cur.execute(record.sql)
            except sqlite3.OperationalError as e:
                status = False
                self.failed_statement_num += 1
                logging.debug("Statement '%s' execution error: %s",record.sql, e)
            
            self.handle_stmt_result(status, record)
            self.con.commit()
        elif type(record) is Query:
            self.query_num += 1
            results = []
            try:
                res = self.cur.execute(record.sql)
                results = res.fetchall()
            except sqlite3.OperationalError as e:
                self.failed_query_num += 1
                logging.debug("Query '%s' execution error: %s",record.sql, e)
            self.handle_query_result(results, record)
            

class DuckDBRunner(Runner):
    def __init__(self, records: List[Record] = []) -> None:
        super().__init__(records)
        self.con = None

    def connect(self, file_path):
        logging.info("connect to db %s", file_path)
        self.con = duckdb.connect(database=file_path)
    
    def close(self):
        self.con.close()
    
    def _single_run(self, record: Record):
        # print(record.sql)
        
        if 'duckdb' not in record.db:
            # print("skip this statement")
            return
        self.total_sql += 1
        if type(record) is Statement:
            self.statement_num += 1
            status = True
            try:
                res = self.con.execute(record.sql)
            except duckdb.ProgrammingError as e:
                status = False
                self.failed_statement_num +=1
                logging.debug(
                    "Statement '%s' execution error: %s", record.sql, e)
            except duckdb.DataError as e:
                status = False
                self.failed_statement_num +=1
                logging.debug("Statement '%s' execution error: %s", record.sql, e)
            self.handle_stmt_result(status, record)
        elif type(record) is Query:
            self.query_num += 1
            results = []
            try:
                self.con.execute(record.sql)
                results = self.con.fetchall()
            except duckdb.ProgrammingError as e:
                self.failed_query_num += 1
                logging.debug("Query '%s' execution error: %s",record.sql, e)
            except duckdb.DataError as e:
                self.failed_query_num += 1
                logging.debug("Query '%s' execution error: %s", record.sql, e)
            # print(results)
            self.handle_query_result(results, record)
    
class CockroachDBRunner(Runner):
    def __init__(self, records: List[Record]) -> None:
        super().__init__(records)
        self.con = None