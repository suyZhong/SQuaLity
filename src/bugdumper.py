from datetime import datetime
from .utils import *
import sqlite3
from typing import List
import pandas as pd
import logging


class BugDumper():
    """TODO Can we use perhaps pandas? Would it be easier?"""
    def __init__(self, dbms_name, dump_all) -> None:
        self.conn = sqlite3.connect("database.db")
        self.cur = self.conn.cursor()
        self.output = "output/demo.csv"
        self.tables = ['DBMS_BUGS', 'BUG_LOGS', 'BUG_TEST_CASES']
        self.views = ['DBMS_BUGS_STATUS', 'ORACLES_AGGREGATED', 'TAGS_AGGREGATED', 'DBMS_BUGS_TRUE_POSITIVES', 'BUG_TEST_CASES_NO_FP', 'DBMS_BUGS_FALSE_POSITIVES', 'TAGS_AGGREGATED_WITH_FP']
        self.dbms_name = dbms_name
        self.dump_all = dump_all
        self.init_bugs_schema()
    
    def reset_schema(self):
        self.bugs_dataframe = pd.DataFrame(columns=self.bugs_columns)
        self.logs_dataframe = pd.DataFrame(columns=self.logs_columns)
    
    def init_bugs_schema(self):
        # The basic bugs schema
        self.bugs_columns = ['DBMS_NAME','TESTFILE_INDEX', 'TESTFILE_PATH',
                             'TESTCASE_INDEX', 'SQL', 'CASE_TYPE', 'EXPECTED_RESULT', 'ACTUAL_RESULT','EXEC_TIME', 'DATE','LOGS_INDEX']
        self.bugs_single_row = dict().fromkeys(self.bugs_columns)
        self.bugs_dataframe = pd.DataFrame(columns=self.bugs_columns)
        
        # The log schema
        self.logs_columns = ['LOGS']
        self.log_index = 0
        self.logs_single_row = dict().fromkeys(self.logs_columns)
        self.logs_dataframe = pd.DataFrame(columns=self.logs_columns)
        
        self.bugs_dataframe.to_csv("output/%s_bugs.csv" % self.dbms_name, mode="w", header=True)
        self.logs_dataframe.to_csv("output/%s_logs.csv" % self.dbms_name, mode="w", header=True, index=False)
        
    def get_testfile_data(self, *args, **kwargs):
        self.testfile_path = kwargs['testfile_path']
        self.testfile_index = kwargs['testfile_index']
        # self.testfile_content = kwargs['testfile_content']
        
    
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
        
    
    def save_state(self, logs:List[Statement], record:Record, result, execution_time:int):
        # record the log
        temp_log = ";\n".join([log.sql for log in logs])
        new_log_flag = (temp_log != self.logs_single_row['LOGS'])
        
        # print(self.bugs_single_row)
        # print(self.logs_single_row)
        # append to the dataframe
        if new_log_flag:
            self.log_index += 1
            self.logs_single_row['LOGS'] = temp_log
            self.logs_dataframe = pd.concat([self.logs_dataframe, pd.DataFrame([self.logs_single_row])], ignore_index = True)
            
        
        # record the bug according to bugs schema
        self.bugs_single_row['DBMS_NAME'] = self.dbms_name
        self.bugs_single_row['TESTFILE_INDEX'] = self.testfile_index
        self.bugs_single_row['TESTFILE_PATH'] = self.testfile_path
        self.bugs_single_row['TESTCASE_INDEX'] = record.id
        self.bugs_single_row['SQL'] = record.sql
        self.bugs_single_row['CASE_TYPE'] = type(record).__name__
        self.bugs_single_row['EXPECTED_RESULT'] = record.result
        self.bugs_single_row['ACTUAL_RESULT'] = result.strip() # notice the result is a string
        self.bugs_single_row['DATE'] = datetime.now().strftime("%y-%m-%d-%H:%M")
        self.bugs_single_row['EXEC_TIME'] = execution_time
        
        
        self.bugs_single_row['LOGS_INDEX'] = self.log_index - 1
        self.bugs_dataframe = pd.concat([self.bugs_dataframe, pd.DataFrame([self.bugs_single_row])], ignore_index = True)
        
    def output_single_state(self, logs:List[Statement], record:Record):
        myDebug("Find a potential bug! Let's see the log and record.")
        for log in logs:
            myDebug(log.sql)
        myDebug(record.sql)
        
    def print_state(self):
        print(self.bugs_dataframe)
        print(self.logs_dataframe)
    
    def dump_to_csv(self, dbname='demo', mode='a'):
        myDebug("Dump the bugs as a Dataframe to a csv %s" % dbname)
        self.bugs_dataframe.to_csv("output/%s_bugs.csv" % dbname, mode=mode, header=False)
        self.logs_dataframe.to_csv("output/%s_logs.csv" % dbname, mode=mode, header=False, index=False)
