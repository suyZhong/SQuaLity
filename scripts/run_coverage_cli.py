import argparse
import os
import sys
import subprocess
from tqdm import tqdm
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src.testcollector import TestcaseCollector, find_local_tests
from src import utils
from src import testparser
from src import testrunner

def extract_and_run(dbms_name: str, parser: testparser.Parser, runner: testrunner.CLIRunner):
    test_files = find_local_tests(dbms_name)

    for test_file in tqdm(test_files):
        # parse the file to get records
        print(test_file)
        parser.get_file_name(test_file)
        parser.get_file_content()
        parser.parse_file()

        records = parser.get_records()
        print(len(records))
        
        # print(runner.cmd)
        print("Running coverage for test file: ", test_file)
        
        all_sql = "".join([record.sql + ";\n" for record in records])
        # Use subprocess to run the SQL queries
        # process = subprocess.Popen(runner.cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        # process.stdin.write(all_sql.encode())
        # process.stdin.close()
        # try:
        #     process.wait(timeout=10)
        # except subprocess.TimeoutExpired:
        #     process.kill()
        #     process.wait()
        
        with open('logs/temp.sql', 'w') as f:
            for record in records:
                f.write(record.sql + ';\n')
        try:                
            subprocess.run(runner.cmd + ' < logs/temp.sql', stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL, shell=True, timeout=100)
        except subprocess.TimeoutExpired:
            print("Timeout")
            continue
        

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', '--suite', choices={
                            'sqlite', 'duckdb', 'cockroachdb', 'all', 'postgresql', 'mysql'}, default='all')
    arg_parser.add_argument('--cli', type=str, default='sqlite3', help='CLI to run the test')
    args = arg_parser.parse_args()
    suite = args.suite
    cli_cmd = args.cli

    cli_runner = testrunner.CLIRunner()
    # cli_runner.cmd = "/home/suyang/Projects/SQuaLity-sigmod/resources/SQLite-b2534d8d/sqlite3"
    # cli_runner.cmd = "/home/suyang/Projects/SQuaLity-sigmod/resources/duckdb/build/coverage/duckdb"
    cli_runner.cmd = cli_cmd
    # Extract SQL Logic Test (SQLite testcase)
    if suite == 'sqlite' or suite == 'all':
        slt_parser = testparser.SLTParser()
        extract_and_run('sqlite', slt_parser, cli_runner)

    # Extract DuckDB Test
    if suite == 'duckdb' or suite == 'all':
        ddt_parser = testparser.DTParser()
        extract_and_run('duckdb', ddt_parser, cli_runner)
        
    # Extract PostgreSQL Test
    if suite == 'postgresql' or suite == 'all':
        psql_parser = testparser.PGTParser()
        extract_and_run('postgresql', psql_parser, cli_runner)