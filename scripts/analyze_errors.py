#!/usr/bin/env python3

import sqlparse

import os
import sys
import argparse
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src import testanalyzer
from src import utils
import matplotlib.pyplot as plt
import pandas as pd


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dbms', choices={'sqlite', 'mysql','duckdb', 'cockroachdb', 'all', 'postgresql', 'psql'}, default='all')
    parser.add_argument('-f', '--folder', default=None)
    parser.add_argument('-c', '--cluster', action='store_true')
    parser.add_argument('-dep', '--dependency', action='store_true')
    parser.add_argument('-sub', '--subset', action='store_true')
    parser.add_argument('-sf', '--suffix', default=None, type=str)
    
    args = parser.parse_args()
    dbms_name = args.dbms
    dir = args.folder
    suffix = args.suffix
    res_analyzer = testanalyzer.TestResultAnalyzer()
    res_analyzer.load_results(dbms_name, dir, suffix)
    
    print(res_analyzer.results.info())
    print(res_analyzer.logs.info())
    
    # get errors
    res_analyzer.get_error_rows()
    print(res_analyzer.errors.info())
    if args.cluster:
        # cluster the error messages
        clusters = 5
        res_analyzer.cluster_error_reasons(n_clusters=clusters)
        errors = res_analyzer.get_error_rows()
        results = res_analyzer.results
        for i in range(clusters):
            print(f"------------------- Cluster {i}: {len(errors[errors['CLUSTER'] == i])} errors --------------------")
            print(results[results['CLUSTER'] == i][['TESTFILE_PATH', 'SQL', 'ERROR_MSG']])

        # compute the similarity between the actual result and the expected result for each error
        res_analyzer.cluster_result_mismatch(n_clusters=clusters)
        errors = res_analyzer.get_error_rows()
        results = res_analyzer.results
        for i in range(100, clusters + 100):
            print(f"------------------- Cluster {i}: {len(errors[errors['CLUSTER'] == i])} errors --------------------")
            error_rows = results[results['CLUSTER'] == i]

        # dump the errors to a csv file
        res_analyzer.dump_errors()

        # dump the clustered results to a csv file
        res_analyzer.dump_results()
    
    if args.dependency:
        for testfile in res_analyzer.results['TESTFILE_PATH'].unique():
            print(f"Extracting dependency failure for {testfile}")
            res_analyzer.extract_dependency_failure(testfile)
        res_analyzer.dump_results('_dependency')
    
    if args.subset:
        # get the subset of the testcases
        case_analyzer = testanalyzer.TestCaseAnalyzer()
        reuse_propotions = []
        output_path = utils.OUTPUT_PATH['testcase_dir'] + dbms_name + '_success'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        for testfile in res_analyzer.results['TESTFILE_PATH'].unique():
            print(f"Extracting subset for {testfile}")
            dbms_suite = testfile.split('_')[0]
            case_analyzer.load_testcases(file_name=(utils.OUTPUT_PATH['testcase_dir'] + utils.DBMS_MAPPING[dbms_suite] + '/'+ utils.convert_testfile_name(testfile, dbms_suite)))
            success_test_case_index = res_analyzer.extract_success_subset(testfile)
            success_test_cases = case_analyzer.extract_subset(success_test_case_index)
            # dump the subset of the testcases
            reuse_propotion = len(success_test_case_index) / len(case_analyzer.test_cases)
            # print(success_test_cases['SQL'])
            print(reuse_propotion)
            reuse_propotions.append(reuse_propotion)
            if reuse_propotion > 0:
                case_analyzer.dump_subset(success_test_case_index, utils.OUTPUT_PATH['testcase_dir'] + utils.DBMS_MAPPING[dbms_name] + '_success' + '/'+ utils.convert_testfile_name(testfile, dbms_suite))
                # exit(0)
        # plt.boxplot(reuse_propotions)
        # plt.savefig('data/reuse_propotion.png')