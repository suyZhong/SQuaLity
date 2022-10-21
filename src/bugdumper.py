from .utils import *
import sqlite3
from typing import List
import logging


class BugDumper():
    """TODO Can we use perhaps pandas? Would it be easier?"""
    def __init__(self) -> None:
        self.conn = sqlite3.connect("database.db")
        self.cur = self.conn.cursor()
        self.tables = ['DBMS_BUGS', 'BUG_TAGS', 'BUG_TEST_CASES']
        self.views = ['DBMS_BUGS_STATUS', 'ORACLES_AGGREGATED', 'TAGS_AGGREGATED', 'DBMS_BUGS_TRUE_POSITIVES', 'BUG_TEST_CASES_NO_FP', 'DBMS_BUGS_FALSE_POSITIVES', 'TAGS_AGGREGATED_WITH_FP']
    
    def init_bugDB(self):
        for table in self.tables:
            self.cur.execute('DROP TABLE IF EXISTS ' + table)
        for view in self.views:
            self.cur.execute('DROP VIEW IF EXISTS '+ view)
        self.cur.execute("""
        CREATE TABLE DBMS_BUGS (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        DATABASE STRING,
        DATE STRING,
        TEST STRING,
        TRIGGER_BUG_LINE STRING,
        UNIQUE_BUG STRING,
        SEVERITY STRING,
        URL_EMAIL STRING,
        URL_BUGTRACKER STRING,
        URL_FIX STRING
        );
        """)
        
    def recover_db(self):
        pass
        
        
        
    def output_single_state(self, logs:List[Statement], record:Record):
        myDebug("Find a potential bug! Let's see the log and record.")
        for log in logs:
            myDebug(log.sql)
        myDebug(record.sql)
