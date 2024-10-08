from os import listdir
from src import testrunner
import pandas as pd
import sys

from src.utils import Query, Statement, Control, RunnerAction, SortType, ResultFormat
import logging



def test_run_test(tmp_path, caplog):
    testcase_path = "data/cornercase/"
    files = listdir(testcase_path)
    def exit_on_error(record):
        if record.levelno >= logging.CRITICAL:
            print(record)
            sys.exit(1)
    logger = logging.getLogger()
    logger.addFilter(exit_on_error)
    runner = testrunner.DuckDBRunner()
    runner.init_dumper()
    runner.init_filter()

    for file in files:
        compression = 'zip' if file.endswith('.zip') else None
        testcases = pd.read_csv(testcase_path + file,
                                compression=compression, na_filter=False).fillna("")

        records = []
        for i, row in testcases.iterrows():
            if row.TYPE == "STATEMENT":
                record = Statement(sql=row.SQL, status=row.STATUS,
                                   id=row.INDEX, result=str(row.RESULT))
            elif row.TYPE == "QUERY":
                record = Query(sql=row.SQL, result=row.RESULT, data_type=row.DATA_TYPE, sort=SortType(
                    int(row.SORT_TYPE)), id=row.INDEX, res_format=ResultFormat(int(row.RES_FORM)))
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
