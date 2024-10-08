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
    arg_parser.add_argument('-d', '--dbms', type=str, default='sqlite', help='DBMS to fuzz')
    arg_parser.add_argument('-m', '--mode', type=str, default='mutation', help='fuzzing mode')
    arg_parser.parse_args()
    args = arg_parser.parse_args()
    
    # get current time
    time_string = time.strftime("%Y-%m-%d-%H-%M", time.localtime())
    if os.path.exists('logs/fuzzing') == False:
        os.makedirs('logs/fuzzing')
    logging.basicConfig(level=getattr(logging, args.log.upper(
    )), format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename=f'logs/fuzzing/{args.dbms}-{time_string}.log')
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename='logs/debug.log')
    if args.dbms == 'sqlite':
        fuzzer = fuzzer.SQLiteSimpleFuzzer(seed=args.seed, mode = args.mode)
    elif args.dbms == 'duckdb':
        fuzzer = fuzzer.DuckDBSimpleFuzzer(seed=args.seed, mode = args.mode)
    elif args.dbms == 'psql':
        fuzzer = fuzzer.PostgreSQLSimpleFuzzer(seed=args.seed, mode = args.mode)
    elif args.dbms == 'mysql':
        fuzzer = fuzzer.MySQLSimpleFuzzer(seed=args.seed, mode = args.mode)
    else:
        assert False, 'DBMS not supported'
    fuzzer.fuzz(args.corpus)