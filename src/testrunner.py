import imp
import re
from typing import List
from .utils import *

import sqlite3

class Runner():
    def __init__(self, records:List[Record]) -> None:
        self.records = records
        
    def run():
        pass
    

class SQLiteRunner(Runner):
    def __init__(self, records: List[Record], db) -> None:
        super().__init__(records)
        self.db = db
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
        
    
    # def connect(self):
    #     con = sqlite3.connect(self.db)
    #     self.cur = con.cursor()
    
    def run(self):
        for record in self.records:
            if 'sqlite' in record.db:
                self._single_run(record)
        
    def _single_run(self, record:Record):
        print(record.sql)
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
