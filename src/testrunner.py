from dataclasses import dataclass
import os
from typing import List
from .utils import *

import sqlite3
import duckdb

class Runner():
    def __init__(self, records:List[Record]) -> None:
        self.records = records
        
    def run(self):
        for record in self.records:
            self._single_run(record)
    
    def get_records(self, records:List[Record]):
        self.records = records
        
    def _single_run(record):
        pass
        
    def debug_run(self, iter=1):
        for i, record in enumerate(self.records):
            if i > iter:
                break
            print(record.sql)
            self._single_run(record)
        

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
            
            # Let's do it in a lazy way (just see if all the items appear)
            results_set = set()
            for row in results:
                for item in row:
                    results_set.add(str(item))
            # print('-------')
            gt_set = set(record.result.split())
            if len(gt_set - results_set) == 0:
                print("True")
            else:
                print("False")
                print(gt_set, results_set)

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
            return
        if type(record) is Statement:
            res = self.con.execute(record.sql)
            # self.con.commit()
        elif type(record) is Query:
            self.con.execute(record.sql)
            results = self.con.fetchall()

            # Let's do it in a lazy way (just see if all the items appear)
            results_set = set()
            for row in results:
                for item in row:
                    results_set.add(str(item))
            # print('-------')
            gt_set = set(record.result.split())
            if len(gt_set - results_set) == 0:
                print("True")
            else:
                print("False")
                print(gt_set, results_set)
                
    # def set_dbfile(self, file_path):