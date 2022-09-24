import os
import re
import argparse
import pandas as pd

STMT_KEYWORDS = ['DROP', 'CREATE', 'SET',
                 'INSERT', 'REPLACE', 'UPDATE', 'END', 'DELETE']
QUERY_KEYWORDS = ['SELECT', 'SHOW']

stmt_reg = re.compile(r'DROP|CREATE|SET|INSERT|REPLACE|UPDATE|END|DELETE')
query_reg = re.compile(r'SELECT|SHOW')
error_reg = re.compile(r'ERROR')


def extractMysqlTestCase(test_case):
    test_file_name = 't/' + test_case + '.test'
    result_file_name = 'r/' + test_case + '.result'

    statements = []
    results = []
    content_by_lines = []

    # process .test file, get a list of SQL statements
    with open(test_file_name, 'r') as f:
        content_by_lines = f.readlines()
        # f.seek(0)
        # content = (f.read()).split(';\n')

    # remove all the Mysqltest Commands and Comments
    # statements = [line.rstrip() for line in content_by_lines if line[0:2] != '--' and line [0:1] != '#']

    # tricky way as I found all SQL statement should be Uppercase
    statements = [line.rstrip()
                  for line in content_by_lines if line[0] <= 'Z' and line[0] >= 'A']
    print(statements[0:10])

    #process .result file, get the stmt matching result
    with open(result_file_name, 'r') as f:
        content_by_lines = f.readlines()
    results = [line.rstrip() for line in content_by_lines]
    print(results[0:10])

    keywords_set = set()
    for item in statements:
        keywords_set.add(item.split()[0])
    print(keywords_set)

    # compare with the later content to see if stmt is ok
    # to show the result.
    # forward_ptr = 1
    # for ind, item in enumerate(statements):
    #     if (whole_results[ind + forward_ptr] == statements[ind + 1]):
    #         results.append('statement ok')

    return statements, results


'''
require all the SQL statements and queries are uppercase.
'''


def parseMysqltestResult(test_case):
    result_file_name = 'r/' + test_case + '.result'

    statements = []
    results = []

    # open result file and readlines
    with open(result_file_name, 'r') as f:
        content_by_lines = f.readlines()

    ind = 0

    # consider the boundary conditions
    while ind < len(content_by_lines) - 1:
        line = content_by_lines[ind]
        next_line = content_by_lines[ind + 1]
        # find statement; consider if next is ERROR msg
        if re.match(stmt_reg, line):
            statements.append(line.rstrip())
            if re.match(error_reg, next_line):
                results.append('# ' + next_line + 'statement error')
                ind += 1
            else:
                results.append('statement ok')
        # find query; add to result till next SQL
        elif re.match(query_reg, line):
            tmp_result = []
            statements.append(line.rstrip())
            while not re.match(stmt_reg, next_line) and not re.match(query_reg, next_line):
                tmp_result.append(next_line)
                ind += 1
                next_line = content_by_lines[ind + 1]
            results.append(tmp_result)
            # continue
        else:
            print('skip this line - ' + line)
        ind += 1
    # if the last line is a statement
    if ind == len(content_by_lines) - 1:
        statements.append(content_by_lines[-1])
        results.append('statement ok')

    print(statements[-5:-1])
    print(results[-5:-1])
    return statements, results


def format_result(result_list):
    # result_lines = ""
    # for line in result_list:
    # result_lines.join(line)
    return "".join(result_list)


def outputToFile(path, statements, results):
    with open(path, 'w') as f:
        for ind, stmt in enumerate(statements):
            if re.match(stmt_reg, stmt):
                f.write(results[ind]+'\n')
                f.write(stmt[:-1] + '\n')
            else:
                f.write('query' + '\n')
                f.write(stmt[:-1] + '\n')
                f.write('----\n')
                f.write(format_result(results[ind]) + '\n')
            f.write('\n')


if __name__ == "__main__":
    working_directory = '/home/suyuz/sqltests/mst2lst/'
    test_suite = 'funcs/'
    test_case = 'tc_rename'
    output_path = 'slt/' + test_case + '.slt'
    os.chdir(working_directory + test_suite)

    # to show the original test in vscode.
    os.system('code %s' % ('r/' + test_case + '.result'))
    # statements, results = extractMysqlTestCase(test_case)
    statements, results = parseMysqltestResult(test_case)
    outputToFile(output_path, statements, results)
