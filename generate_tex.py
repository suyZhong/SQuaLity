#!/usr/bin/env python3

import pandas as pd
from src import testcollector

BASE_PATH = "../SQuaLity-Paper/assets/"

TEST_SUITES_SUMMARY = """\centering
\caption{{DBMS rankings and their test suites information}}\label{{table:suties}}
\\begin{{tabular}}{{ l|c|c|c }}
\hline
DBMS & DB-Engines & GitHub Stars & Test Files  \\\ 
\hline
SQLite & 10 & 3.2k & {sqlite_test_files}\\\ 
MySQL & 2 & 8.5k & {mysql_test_files}  \\\ 
PostgreSQL & 4 & 11.3k & {postgres_test_files}\\\ 
CockroachDB & 56 & 26.1k & {cockroach_test_files} \\\ 
DuckDB & 171 & 7.4k & {duckdb_test_files}\\\ 
\hline
\end{{tabular}}"""


def generate_test_suites_summary():
    path = BASE_PATH + "table/TestSuitesTable.tex"

    data = dict()

    data['sqlite_test_files'] = len(testcollector.find_local_tests('sqlite'))
    data['mysql_test_files'] = len(testcollector.find_local_tests('mysql'))
    data['postgres_test_files'] = len(
        testcollector.find_local_tests('postgres'))
    data['cockroach_test_files'] = len(
        testcollector.find_local_tests('cockroach'))
    data['duckdb_test_files'] = len(testcollector.find_local_duckdb_test())
    content = TEST_SUITES_SUMMARY.format(**data)

    with open(path, 'w') as f:
        f.write(content)


if __name__ == '__main__':
    generate_test_suites_summary()
