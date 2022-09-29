import os
from src import testparser
from src import testrunner
import argparse

def find_tests(db_name:str):
    db_name = db_name.lower()

    test_suite_dir = db_name + "_tests/"
    if db_name == "cockroach":
        test_suite_dir += 'testdata/logic_test'
    elif db_name == "duckdb":
        test_suite_dir += 'sql'
    elif db_name == "mysql":
        test_suite_dir += 'r'
    elif db_name == "postgres":
        test_suite_dir += 'regress/expected'
    elif db_name == "sqlite":
        test_suite_dir += ''
    else:
        assert ('Not supported db')

    tests_files = []
    print("walk in " + test_suite_dir)
    g = os.walk(test_suite_dir)
    for path, dir_list, file_list in g:
        tests_files += [os.path.join(path, file_name) for file_name in file_list]
    return tests_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--dbms", type=str,help="Enter the DBMS name")
    parser.add_argument('-t',"--db_test", type=str,default="sqlite", help="Enter the db test cases")
    parser.add_argument('-f',"--db_file", type=str, default="output/test.db", help="Enter the in-mem db file save path")
    parser.add_argument("--max_files", type=int, default=-1, help="Max test files it run")
    
    
    args= parser.parse_args()
    dbms_name = str.lower(args.dbms)
    testcase_name = str.lower(args.db_test)
    max_files = args.max_files
    db_file = args.db_file
    test_files = find_tests(testcase_name)
    
    # set the runner
    if dbms_name == 'sqlite':
        r = testrunner.SQLiteRunner(db=db_file)
    elif dbms_name == 'duckdb':
        r = testrunner.DuckDBRunner(db=db_file)
    else:
        exit("Not implement yet")
    
    # set the parser
    if testcase_name == 'sqlite':
        p = testparser.SLTParser()
    else:
        exit("Not implement yet")
    
    for i, test_file in enumerate(test_files):
        if max_files > 0 and i > max_files:
            break
        print("test file", i)
        print("-----------------------------")
        print("parsing", test_file)
        p.get_file_name(test_file)
        p.get_file_content()
        p.parse_file()
        os.system('rm %s' % db_file)
        
        r.get_records(p.get_records())
        r.connect(db_file)
        print("-----------------------------")
        print("running", test_file)
        r.run()
        if not r.allright:
            print("Wrong!", test_file)
        print ("##########################\n\n")