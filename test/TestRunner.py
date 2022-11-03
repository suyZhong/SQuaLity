from os import listdir
from src import testrunner
import pandas as pd

from src.utils import Query, Statement, Control, RunnerAction, SortType
import logging



def test_run_test(tmp_path, caplog):
    testcase_path = "data/cornercase/"
    files = listdir(testcase_path)

    runner = testrunner.DuckDBRunner()
    runner.init_dumper()

    for file in files:
        testcases = pd.read_csv(testcase_path + file,
                                compression='zip', na_filter=False).fillna("")

        records = []
        for i, row in testcases.iterrows():
            if row.TYPE == "STATEMENT":
                record = Statement(sql=row.SQL, status=row.STATUS,
                                   id=row.INDEX, result=str(row.RESULT))
            elif row.TYPE == "QUERY":
                record = Query(sql=row.SQL, result=row.RESULT, data_type=row.DATA_TYPE, sort=SortType(
                    int(row.SORT_TYPE)), id=row.INDEX)
            elif row.TYPE == "CONTROL":
                record = Control(action=RunnerAction(int(row.SQL)))
            record.set_execute_db(row.DBMS.split(','))
            records.append(record)
        db_path = str(tmp_path) + "tempdb"
        runner.set_db(db_path)
        runner.get_records(records, 0, testcase_path)
        runner.connect(db_path)
        runner.run()
        runner.dump()
    # assert 0
