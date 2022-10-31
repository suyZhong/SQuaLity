#!/usr/bin/env python3

import argparse
import os
import sys
import logging
from datetime import datetime


from src import testparser
from src import testrunner
from src.utils import DBMS_Set


def find_tests(test_suite: str):
    test_suite = test_suite.lower()

    test_suite_dir = test_suite + "_tests/"
    if test_suite == "cockroach":
        test_suite_dir += 'testdata/logic_test'
    elif test_suite == "duckdb":
        test_suite_dir += 'sql'
    elif test_suite == "mysql":
        test_suite_dir += 'r'
    elif test_suite == "postgres":
        test_suite_dir += 'regress/expected'
    elif test_suite == "sqlite":
        test_suite_dir += ''
    else:
        sys.exit("Test suite not support!")

    tests_files = []
    print("walk in " + test_suite_dir)
    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list]
    return tests_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--dbms", choices=DBMS_Set, default='duckdb',
                        type=str, help="Enter the DBMS name")
    parser.add_argument('-s', "--suite_name", type=str, choices=DBMS_Set,
                        default="sqlite", help="Enter the dbms test suites")
    parser.add_argument('-t', '--test_file', type=str,
                        default="", help="test a specific file")
    parser.add_argument('-f', "--db_name", type=str, default="output/tempdb",
                        help="Enter the database name. For embedded database it's the file path.")
    parser.add_argument('-u', "--db_url", type=str,
                        default="postgresql://root@localhost:26257/defaultdb?sslmode=disable",
                        help="Enter the Dabase url")
    parser.add_argument('--log', type=str, default="DEBUG",
                        help="logging level")
    parser.add_argument("--max_files", type=int, default=0,
                        help="Max test files it run. Negative value means skipping the absolute number of test files")
    parser.add_argument('--dump_all', action='store_true',
                        help="If added, it would dump every testcases to csv, besides error cases.")

    args = parser.parse_args()
    dbms_name = str.lower(args.dbms)
    suite_name = str.lower(args.suite_name)
    max_files = args.max_files
    db_url = args.db_url
    db_name = args.db_name
    test_file = args.test_file
    log_level = args.log
    if test_file:
        test_files = [test_file]
    else:
        test_files = find_tests(suite_name)
    file_num = len(test_files)
    begin_time = datetime.now()
    log_file = "logs/" + dbms_name + '_' + suite_name + \
        '-' + begin_time.strftime("%m-%d-%H%M")

    log_file += ".log"
    if log_level != "DEBUG":
        logging.basicConfig(filename=log_file, encoding='utf-8',
                            level=getattr(logging, log_level.upper()),)
        sys.stdout = open(log_file + ".out", "a", encoding='utf-8')
    else:
        logging.basicConfig(filename="logs/debug.log", encoding='utf-8',
                            level=getattr(logging, log_level.upper()), filemode='w')
        sys.stdout = open("logs/debug" + ".out", "w", encoding='utf-8')

    # set the runner
    if dbms_name == 'sqlite':
        r = testrunner.SQLiteRunner()
    elif dbms_name == 'duckdb':
        r = testrunner.DuckDBRunner()
    elif dbms_name == 'cockroachdb':
        r = testrunner.CockroachDBRunner()
    elif dbms_name == 'mysql':
        r = testrunner.MySQLRunner()
    else:
        sys.exit("Not implement yet")
    r.init_dumper(dump_all=args.dump_all)

    # set the parser
    if suite_name == 'sqlite':
        p = testparser.SLTParser()
    elif suite_name == 'duckdb':
        p = testparser.DTParser()
    else:
        sys.exit("Not implement yet")

    skip_index = [140]
    for i, test_file in enumerate(test_files):
        db_name = args.db_name + str(i)
        single_begin_time = datetime.now()
        if i in skip_index:
            continue
        if max_files <= 0 and i < abs(max_files):
            continue
        if 0 < max_files < i:
            break
        # print("-----------------------------------")
        logging.info("test file %d", i)
        logging.info("parsing %s", test_file)
        p.get_file_name(test_file)
        p.get_file_content()
        p.parse_file()

        r.set_db(db_name)
        r.get_records(p.get_records(), testfile_index=i,
                      testfile_path=test_file)
        r.connect(db_name)
        # print("-----------------------------------")
        logging.info("running %s", test_file)
        try:
            r.run()
        except r.db_error as e:
            logging.critical(
                "Runner catch an exception %s , it is either the runner's bug or the connector's bug.", e)
        r.close()
        if log_level != "DEBUG":
            r.remove_db(db_name)
        single_end_time = datetime.now()
        single_running_time = (single_end_time - single_begin_time).seconds
        r.running_summary(str(i) + " " + test_file, single_running_time)
        # print ("#############################\n\n")
        r.dump()
    r.running_summary("ALL", (datetime.now()-begin_time).seconds)
