from dataclasses import dataclass
import os
from typing import List
from .utils import *
import hashlib

import sqlite3
import duckdb

class Runner():
    def __init__(self, records:List[Record]) -> None:
        self.records = records
        self.hash_threshold = 8
        self.allright = True
        
    def run(self):
        for record in self.records:
            self._single_run(record)
    
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
    
    def handle_query_result(self, results:list, record:Record):
        result_string = ""
            # get result length
        if results:
            result_len = len(results) * len(results[0])
            # sort result and output flat list
            result_string = self._sort_result(results, sort_type=record.sort)
        else:
            result_len = 0
        result_string = self._replace_None(result_string)
        
        if result_len >= self.hash_threshold:
            result_string = self._hash_results(result_string)
            result_string = str(result_len) + " values hashing to " + result_string
        
        
        if result_string.strip() == record.result.strip():
            # print("True!")
            pass
        else:
            print(record.sql)
            print("False")
            print(results, result_string)
            print(record.result)
            self.allright = False
        

class SQLiteRunner(Runner):
    def __init__(self, records: List[Record]=[], db="demo.db") -> None:
        super().__init__(records)
        self.db = db
        self.con = None
        self.cur = None
        
    
    def connect(self, file_path):
        self.db = file_path
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
    
    
    def set_dbfile(self, file_path):
        self.db = file_path

    
    def _single_run(self, record:Record):
        # print(record.sql)
        if 'sqlite' not in record.db:
            return
        if type(record) is Statement:
            res = self.cur.execute(record.sql)
            self.con.commit()
        elif type(record) is Query:
            res = self.cur.execute(record.sql)
            results = res.fetchall()
            self.handle_query_result(results, record)
            

class DuckDBRunner(Runner):
    def __init__(self, records: List[Record] = [], db = "demo.db") -> None:
        super().__init__(records)
        self.db = db
        self.con = None

    def connect(self, file_path):
        self.db = file_path
        self.con = duckdb.connect(database=self.db)
    
    def _single_run(self, record: Record):
        # print(record.sql)
        
        if 'duckdb' not in record.db:
            # print("skip this statement")
            return
        if type(record) is Statement:
            res = self.con.execute(record.sql)
            # self.con.commit()
        elif type(record) is Query:
            self.con.execute(record.sql)
            results = self.con.fetchall()
            # print(results)
            self.handle_query_result(results, record)