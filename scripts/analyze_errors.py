#!/usr/bin/env python3

import sqlparse

import os
import sys
import argparse
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src import testanalyzer
import matplotlib.pyplot as plt
import pandas as pd


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dbms', choices={'sqlite', 'duckdb', 'cockroachdb', 'all', 'postgresql', 'psql'}, default='all')
    
    args = parser.parse_args()
    dbms_name = args.dbms
    res_analyzer = testanalyzer.TestResultAnalyzer()
    res_analyzer.load_results(dbms_name)
    
    print(res_analyzer.results.info())
    print(res_analyzer.logs.info())
    
    # get errors
    res_analyzer.get_error_rows()
    print(res_analyzer.errors.info())
    
    # cluster the error messages
    clusters = 5
    res_analyzer.cluster_error_reasons(n_clusters=clusters)
    errors = res_analyzer.get_error_rows()
    results = res_analyzer.results
    for i in range(clusters):
        print(f"------------------- Cluster {i}: {len(errors[errors['CLUSTER'] == i])} errors --------------------")
        print(results[results['CLUSTER'] == i][['TESTFILE_PATH', 'SQL', 'ERROR_MSG']])
    