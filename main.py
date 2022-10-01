import os
from src import testparser
from src import testrunner
import argparse
import logging
from datetime import datetime
import sys


def find_tests(db_name: str):
    db_name = db_name.lower()

    test_suite_dir = db_name + "_tests/"
    if db_name == "cockroach":
        test_suite_dir += 'testdata/logic_test'
    elif db_name == "duckdb":
        test_suite_dir += 'sql'
    elif db_name == "mysql":
        test_suite_dir += 'r'
    elif db_name == "postgres":
        test_suite_dir += 'regress/expected'
    elif db_name == "sqlite":
        test_suite_dir += ''
    else:
        assert ('Not supported db')

    tests_files = []
    print("walk in " + test_suite_dir)
    g = os.walk(test_suite_dir)
    for path, dir_list, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list]
    return tests_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--dbms", type=str, help="Enter the DBMS name")
    parser.add_argument('-s', "--suite_name", type=str,
                        default="sqlite", help="Enter the dbms test suites")
    parser.add_argument('-t', '--test_file', type=str,
                        default="", help="test a specific file")
    parser.add_argument('-f', "--db_file", type=str, default=":memory:",
                        help="Enter the in-mem db file save path")
    parser.add_argument('--log', type=str, default="", help="logging path")
    parser.add_argument("--max_files", type=int, default=0,
                        help="Max test files it run")

    args = parser.parse_args()
    dbms_name = str.lower(args.dbms)
    suite_name = str.lower(args.suite_name)
    max_files = args.max_files
    db_file = args.db_file
    test_file = args.test_file
    log_level = args.log
    if test_file:
        test_files = [test_file]
    else:
        test_files = find_tests(suite_name)
    file_num = len(test_files)

    log_file = "logs/" + dbms_name + '_' + suite_name + \
        '-' + datetime.now().strftime("%m-%d-%H%M") + ".log"
    if log_level != "DEBUG":
        logging.basicConfig(filename=log_file, encoding='utf-8',
                            level=getattr(logging, log_level.upper()),)
    else:
        logging.basicConfig(filename="logs/debug.log", encoding='utf-8',
                            level=getattr(logging, log_level.upper()),)
    sys.stdout =  open(log_file, "a")

    # set the runner
    if dbms_name == 'sqlite':
        r = testrunner.SQLiteRunner()
    elif dbms_name == 'duckdb':
        r = testrunner.DuckDBRunner()
    else:
        exit("Not implement yet")

    # set the parser
    if suite_name == 'sqlite':
        p = testparser.SLTParser()
    else:
        exit("Not implement yet")

    for i, test_file in enumerate(test_files):
        if max_files <= 0 and i < abs(max_files):
            continue
        db_file = args.db_file + str(i)
        os.system('rm %s' % db_file)
        if max_files > 0 and i > max_files:
            break
        # print("-----------------------------------")
        logging.info("test file %d", i)
        logging.info("parsing %s", test_file)
        p.get_file_name(test_file)
        p.get_file_content()
        p.parse_file()
        r.get_records(p.get_records())
        r.connect(db_file)
        # print("-----------------------------------")
        logging.info("running %s", test_file)
        r.run()
        r.close()
        if r.allright:
            logging.info("Pass all test case!", )
            try:
                os.remove(db_file)
            except:
                logging.error("No such file or directory:", db_file)
        r.running_summary(test_file)
        # print ("#############################\n\n")
