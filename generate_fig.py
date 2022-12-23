#!/usr/bin/env python3

import matplotlib.pyplot as plt
from src import analyzer

BASE_PATH = "../SQuaLity-Paper/assets/"


def generate_sql_distribution(dbms_name: str = 'sqlite'):
    print("Get metadata for {}".format(dbms_name))
    test_analyzer = analyzer.TestCaseAnalyzer()
    test_analyzer.load_testcases('data/{}'.format(dbms_name))
    statements = test_analyzer.get_statements()
    queries = test_analyzer.get_queries()

    print("Total number of statement is", len(statements))
    print("Average statement length is", statements['SQL'].apply(len).mean())

    print("Total number of query is", len(queries))
    print("Average query length is", queries['SQL'].apply(len).mean())
    statements['SQL'] = statements['SQL'].str.upper()
    statements_key = statements['SQL'].str.split(expand=True)[0]
    print("Statement type:")
    stmt_count = statements_key.value_counts(normalize=True)
    # print(stmt_count)

    queries = queries[:1000]

    queries['SQL'] = queries['SQL'].str.upper()
    query_key = queries['SQL'].str.split(expand=True)[0]
    print("Query type:")
    query_count = query_key.value_counts(normalize=True)
    # print(query_count)

    f, (a0, a1) = plt.subplots(1, 2, gridspec_kw={
        'width_ratios': [2, 1]}, figsize=(6, 3))
    query_count[:min(len(query_count), 5)].plot.bar(
        title="Query Commands", ax=a1, color='gray')
    stmt_count[:min(len(stmt_count), 10)].plot.bar(
        title="Statement Commands", ax=a0, color='gray')
    f.tight_layout()
    f.savefig("{}img/{}-testcase".format(BASE_PATH, dbms_name))


if __name__ == '__main__':
    generate_sql_distribution('sqlite')
    generate_sql_distribution('duckdb')
    generate_sql_distribution('cockroach')
