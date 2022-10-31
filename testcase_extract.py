import os
import sys
from .src import testparser


def find_tests(db_name: str):
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
        sys.exit("test suite not supported yet")

    tests_files = []
    print("walk in " + test_suite_dir)
    g = os.walk(test_suite_dir)
    for path, _, file_list in g:
        tests_files += [os.path.join(path, file_name)
                        for file_name in file_list]
    return tests_files

# Extract SQL Logic Test (SQLite testcase)
slt_parser = testparser.SLTParser()
test_files = find_tests("sqlite")

for i, test_file in enumerate(test_files):
    slt_parser.get_file_name(test_file)
    slt_parser.get_file_content()
    slt_parser.parse_file()
