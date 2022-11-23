import os
import argparse
from src import testparser
from src.testcollector import TestcaseCollector, find_local_tests


def extract(dbms_name:str, parser:testparser.Parser, compression:bool = True):
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
        testcase_name = "-".join(test_file.removeprefix(
            "{}_tests/".format(dbms_name)).replace(".test", ".csv").split('/'))
        records = parser.get_records()
        # print(len(records))
        
        collector.init_testcase_schema(dbms_name, testcase_name, compression)
        collector.save_records(records)
        collector.dump_to_csv()
        # exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--suite', choices={'sqlite', 'duckdb', 'cockroach', 'all'}, default='all')
    args = parser.parse_args()
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
    if suite == 'cockroach' or suite == 'all':
        cdbt_parser = testparser.CDBTParser()
        extract('cockroach', cdbt_parser, compression=False)
