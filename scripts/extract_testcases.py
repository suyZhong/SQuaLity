#!/usr/bin/env python3

import argparse
import os
import sys
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src.testcollector import TestcaseCollector, find_local_tests
from src import utils
from src import testparser


def extract(dbms_name: str, parser: testparser.Parser, compression: bool = True):
    collector = TestcaseCollector()
    test_files = find_local_tests(dbms_name)
    os.system("mkdir data/{} -p".format(dbms_name))

    for i, test_file in enumerate(test_files):
        # parse the file to get records
        print(test_file)
        parser.get_file_name(test_file)
        parser.get_file_content()
        parser.parse_file()

        # save the records to a csv
        testcase_name = utils.convert_testfile_name(test_file, dbms_name)
        records = parser.get_records()
        print(len(records))

        collector.init_testcase_schema(dbms_name, testcase_name, compression)
        collector.save_records(records)
        try:
            collector.dump_to_csv()
        except Exception:
            pass
        # exit(0)
    # print(parser.meta_data)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', '--suite', choices={
                            'sqlite', 'duckdb', 'cockroachdb', 'all', 'postgresql', 'mysql'}, default='all')
    args = arg_parser.parse_args()
    suite = args.suite

    # Extract SQL Logic Test (SQLite testcase)
    if suite == 'sqlite' or suite == 'all':
        slt_parser = testparser.SLTParser()
        extract('sqlite', slt_parser)

    # Extract DuckDB Test
    if suite == 'duckdb' or suite == 'all':
        ddt_parser = testparser.DTParser()
        extract('duckdb', ddt_parser, compression=False)

    # Extract CockroachDB test
    if suite == 'cockroachdb' or suite == 'all':
        cdbt_parser = testparser.CDBTParser()
        extract('cockroachdb', cdbt_parser, compression=False)

    # Extract MySQL test
    if suite == 'mysql' or suite == 'all':
        mysql_parser = testparser.MYTParser()
        extract('mysql', mysql_parser, compression=False)

    if suite == 'postgresql' or suite == 'all':
        postgres_parser = testparser.PGTParser()
        extract('postgresql', postgres_parser, compression=False)
