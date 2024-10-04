import argparse
import os
import sys
import subprocess
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src.testcollector import TestcaseCollector, find_local_tests
from src import utils
from src import testparser
from src import testrunner


def set_pg(runner: testrunner.CLIRunner):
    runner.cmd = f"/app/resources/postgres/bin/psql -U postgres -h /tmp -p 10010 -q -c "
    statements = [
        "DROP DATABASE IF EXISTS testdb;\n",
        "CREATE DATABASE testdb;\n",
    ]
    for statement in statements:
        run_cmd = runner.cmd + "\""+ statement+"\""
        subprocess.run(run_cmd, shell=True)
        print(run_cmd)
    runner.cmd = f"/app/resources/postgres/bin/psql -U postgres -h /tmp -d testdb -p 10010 -q"

def extract_and_run(dbms_name: str, parser: testparser.Parser, runner: testrunner.CLIRunner):
    test_files = find_local_tests(dbms_name)

    for i, test_file in enumerate(test_files):
        # parse the file to get records
        print(test_file)
        if test_file == "duckdb_tests/fuzzer/sqlsmith/generate_series_overflow.test":
            continue
        parser.get_file_name(test_file)
        parser.get_file_content()
        parser.parse_file()

        records = parser.get_records()
        print(len(records))
        if dbms_name != 'postgresql':
            set_pg(runner)
        # print(runner.cmd)
        print("Running coverage for test file: ", test_file)
        
        all_sql = "".join([record.sql + ";\n" for record in records])
        all_sql = "SET statement_timeout = '5s';\n" + all_sql
        # Use subprocess to run the SQL queries
        process = subprocess.Popen(runner.cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        process.stdin.write(all_sql.encode())
        process.stdin.close()
        try:
            process.wait(timeout=100)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        
        # with open('logs/temp.sql', 'w') as f:
        #     for record in records:
        #         f.write(record.sql + ';\n')
        # os.system(runner.cmd + ' < logs/temp.sql')
        
        

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-s', '--suite', choices={
                            'sqlite', 'duckdb', 'cockroachdb', 'all', 'postgresql', 'mysql'}, default='all')
    args = arg_parser.parse_args()
    suite = args.suite

    cli_runner = testrunner.CLIRunner()
    cli_runner.cmd = "/app/resources/postgres/bin/psql -U postgres -h /tmp -p 10010 -q"
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