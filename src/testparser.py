from copy import copy
from .utils import *
from typing import List


class Parser:
    def __init__(self, filename='') -> None:
        self.filename = filename
        self.test_content = ""

    # read the whole file

    def get_file_name(self, filename):
        self.filename = filename

    def get_file_content(self):
        with open(self.filename, 'r') as f:
            self.test_content = f.read()

    def parse_file(self):
        pass

    def parse_file_by_lines(self):
        for line in self.test_content:
            self.parse_line(line)

    def parse_line(self, line: str):
        pass


class SLTParser(Parser):
    sort_mode_dict = {'nosort': SortType.NoSort, 
                     'rowsort': SortType.RowSort,
                     'valuesort': SortType.ValueSort}
    
    def __init__(self, filename='') -> None:
        super().__init__(filename)
        self.scripts = ""
        self.records: List[Record] = list()
    
    def _parse_script_lines(self, lines: list):
        # Now the first line are exact command
        line_num = len(lines)
        tokens = lines[0].split()
        record_type = tokens[0]
        r = Statement()

        if record_type == 'statement':
            # assert (line_num <= 2), 'statement too long: ' + '\n'.join(lines)
            status = (tokens[1] == 'ok')
            # Only a single SQL command is allowed per statement
            # r = Statement(sql=lines[1], status=status)
            r = Statement(sql="".join(lines[1:]), status=status)
        elif record_type == 'query':
            # A query record begins with a line of the following form:
            #       query <type-string> <sort-mode> <label>
            data_type = ''
            sort_mode = SortType.NoSort
            label = ''
            # pop out 'query'
            tokens.pop(0)
            # pop out <type-string>
            if tokens:
                data_type = tokens.pop(0)
            # pop out <sort-mode>
            if tokens:
                sort_token = tokens.pop(0)
                try:
                    sort_mode = self.sort_mode_dict[sort_token]
                except KeyError:
                    print("sort mode %s not implemented, change to nosort" %
                          sort_token)
            if tokens:
                label = tokens.pop(0)

            # The SQL for the query is found on second an subsequent lines
            # of the record up to first line of the form "----" or until the
            # end of the record. Lines following the "----" are expected results of the query,
            # one value per line. If the "----" and/or the results are omitted, then the query
            # is expected to return an empty set.
            ind = line_num
            for i, line in enumerate(lines):
                if line == '----':
                    ind = i
                    break
            sql = '\n'.join(lines[1:ind])
            result = ""
            if ind != line_num:
                result = '\n'.join(lines[ind + 1:])

            r = Query(sql=sql, result=result, data_type=data_type,
                      sort=sort_mode, label=label)

        elif record_type == 'skipif':
            tmp_dbms_set = copy(DBMS_Set)
            if tokens[1] in DBMS_Set:
                tmp_dbms_set.remove(tokens[1])
            else:
                # print (DBMS_Set)
                print(
                    "DBMS %s support is stll not implemented, skip nothing" % tokens[1])
                # exit(0)
            r = self._parse_script_lines(lines[1:])
            r.set_execute_db(tmp_dbms_set)
            # print(tmp_dbms_set)
        elif record_type == 'onlyif':
            if tokens[1] in DBMS_Set:
                tmp_dbms_set = set([tokens[1]])
            else:
                print(
                    "DBMS %s support is stll not implemented, skip this script" % tokens[1])
                return
            r = self._parse_script_lines(lines[1:])
            r.set_execute_db(tmp_dbms_set)
        else:
            print("This script has not implement")
            print(lines)
            return
        return r
    
    def parse_script(self, script: str):

        # Lines of the test script that begin with the sharp
        # character ("#", ASCII code 35) are comment lines and are ignored
        lines = [line for line in script.split('\n') if line[0] != '#']
        r = self._parse_script_lines(lines)
        if r:
            self.records.append(r)
        

    # Each record is separated from its neighbors by one or more blank line.

    def parse_file(self):
        self.scripts = [script.strip()
                        for script in self.test_content.split('\n\n') if script != '']
        self.records = []
        for i, script in enumerate(self.scripts):
            self.parse_script(script.strip())

    # TODO Should implement a fucntion to find the location of the error
    def print_scripts(self):
        # print all scripts
        print(self.scripts)
        for i, script in enumerate(self.scripts):
            print(i, script.strip())

    def print_records(self):
        for rec in self.records:
            # rec = Query(rec)
            print('type:', type(rec), type(rec) is Statement)
            print('sql: \n', rec.sql)
            print('result: \n', rec.result)
            print('support db:', rec.db)
            #For query
            # print('data_type: ',rec.data_type)
            # print('label:', rec.label)
            # print('sort:',rec.sort)
    def get_records(self):
        return self.records