import os
from src import testparser
from src.testcollector import TestcaseCollector, find_local_tests


if __name__ == "__main__":
    # Extract SQL Logic Test (SQLite testcase)
    slt_parser = testparser.SLTParser()
    slt_collector = TestcaseCollector()
    test_files = find_local_tests("sqlite")
    os.system("mkdir data/sqlite -p")  # TODO make it be a parameter

    for i, test_file in enumerate(test_files):
        # parse the file to get records
        slt_parser.get_file_name(test_file)
        slt_parser.get_file_content()
        slt_parser.parse_file()

        # save the records to a csv
        testcase_name = "-".join(test_file.removeprefix(
            "sqlite_tests/").replace(".test", ".csv").split('/'))
        records = slt_parser.get_records()
        slt_collector.init_testcase_schema('sqlite', testcase_name)
        slt_collector.save_records(records)
        slt_collector.dump_to_csv()
