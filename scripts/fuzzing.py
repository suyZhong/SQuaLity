import os
import sys
import time
import argparse
import logging
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src import fuzzer



if __name__ == "__main__":
    # init a argument parser
    arg_parser = argparse.ArgumentParser(description='Fuzzing')
    arg_parser.add_argument('--seed', type=int, default=0, help='random seed')
    arg_parser.add_argument('--log', type=str, default='INFO', help='logging level')
    arg_parser.add_argument('--corpus', type=str, default='output/psql_results_dependency_without_error.csv', help='corpus path')
    arg_parser.parse_args()
    args = arg_parser.parse_args()
    
    # get current time
    time_string = time.strftime("%Y-%m-%d-%H-%M", time.localtime())
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename=f'logs/fuzzing-{time_string}.log')
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename='logs/debug.log')
    sqlite_fuzzer = fuzzer.SQLiteSimpleFuzzer()
    sqlite_fuzzer.fuzz(args.corpus)
    # duckdb_fuzzer = fuzzer.DuckDBSimpleFuzzer()
    # duckdb_fuzzer.fuzz('output/psql_results_dependency.csv')