from cProfile import label
from .utils import *
from typing import List


class Parser:
    def __init__(self, filename='') -> None:
        self.filename = filename
        self.test_content = ""

    # read the whole file

    def get_file_content(self):
        with open(self.filename, 'r') as f:
            self.test_content = f.read()

    def parse_file(self):
        len("123")
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
        
    def parse_script(self, script: str):

        # Lines of the test script that begin with the sharp
        # character ("#", ASCII code 35) are comment lines and are ignored
        lines = [line for line in script.split('\n') if line[0] != '#']
        line_num = len(lines)
        tokens = lines[0].split()
        record_type = tokens[0]

        if record_type == 'statement':
            assert (line_num <= 2), 'statement too long: ' + '\n'.join(lines)
            status = (tokens[1] == 'ok')
            # Only a single SQL command is allowed per statement
            r = Statement(sql=lines[1], status=status)
            self.records.append(r)
        elif record_type == 'query':
            # A query record begins with a line of the following form:
            #       query <type-string> <sort-mode> <label>
            data_type = ''
            sort_mode = SortType.NoSort
            label = ''
            tokens.pop(0)
            if tokens:
                data_type = tokens.pop(0)
            if tokens:
                sort_token = tokens.pop(0)
                try:
                    sort_mode = self.sort_mode_dict[sort_token]
                except KeyError:
                    print("sort mode %s not implemented, change to nosort" % sort_token)
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

            r = Query(sql=sql, result=result, data_type=data_type, sort=sort_mode, label=label)
            self.records.append(r)
            
        elif record_type == ''

    # Each record is separated from its neighbors by one or more blank line.

    def parse_file(self):
        self.scripts = [script.strip()
                        for script in self.test_content.split('\n\n') if script != '']
        for i, script in enumerate(self.scripts):
            self.parse_script(script.strip())

    def print_scripts(self):
        # print all scripts
        print(self.scripts)
        for i, script in enumerate(self.scripts):
            print(i, script.strip())

    def print_records(self):
        for rec in self.records:
            # rec = Query(rec)
            print('sql: \n', rec.sql)
            print('result: \n', rec.result)
            
            #For query
            # print('data_type: ',rec.data_type)
            # print('label:', rec.label)
            # print('sort:',rec.sort)
