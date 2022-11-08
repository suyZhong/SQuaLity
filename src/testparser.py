import re
from copy import copy
import pandas as pd
from .utils import *
from typing import List


def strip_comment_lines(code: str):
    return re.sub(r'(?m)^ *#.*\n?', '', code)


def strip_comment_suffix(code: str):
    return re.sub(r'--.*', '', code)


class Parser:
    def __init__(self, filename='') -> None:
        self.filename = filename
        self.test_content = ""
        self.result_content = ""
        self.hash_threshold = 8
        self.records: List[Record] = list()
        self.record_id = 0

    # read the whole file

    def get_file_name(self, filename):
        self.filename = filename

    def get_file_content(self):
        try:
            with open(self.filename, 'r', encoding='utf-8') as f:
                self.test_content = f.read()
        except UnicodeDecodeError:
            with open(self.filename, 'r', encoding='windows-1252') as f:
                self.test_content = f.read()

    def parse_file(self):
        pass

    def testfile_dialect_handler(self, *args, **kwargs):
        return Control(action=RunnerAction.HALT, id=self.record_id)

    def parse_file_by_lines(self):
        for line in self.test_content:
            self.parse_line(line)

    def parse_line(self, line: str):
        pass

    def get_records(self):
        return self.records


class CSVParser(Parser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)

    def get_file_content(self, compression=True):
        self.test_content = pd.read_csv(
            self.filename, compression=compression, na_filter=False).fillna()

    def parse_file(self):
        self.records = []
        for _, row in self.test_content.iterrows():
            if row.TYPE == "STATEMENT":
                record = Statement(sql=row.SQL, status=row.STATUS,
                                   id=row.INDEX, result=str(row.RESULT))
            elif row.TYPE == "QUERY":
                record = Query(sql=row.SQL, result=row.RESULT, data_type=row.DATA_TYPE, sort=SortType(
                    int(row.SORT_TYPE)), id=row.INDEX, label=row.LABEL)
            elif row.TYPE == "CONTROL":
                record = Control(action=RunnerAction(int(row.SQL)))
            record.set_execute_db(row.DBMS.split(','))
            self.records.append(record)


class SLTParser(Parser):
    sort_mode_dict = {'nosort': SortType.NO_SORT,
                      'rowsort': SortType.ROW_SORT,
                      'valuesort': SortType.VALUE_SORT}

    def __init__(self, filename='') -> None:
        super().__init__(filename)
        self.scripts = []
        self.dbms_set = DBMS_Set

    def get_query(self, tokens, lines):
        # A query record begins with a line of the following form:
        #       query <type-string> <sort-mode> <label>
        data_type = ''
        sort_mode = SortType.NO_SORT
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
        ind = len(lines)
        for i, line in enumerate(lines):
            if line == '----':
                ind = i
                break
        sql = '\n'.join(lines[1:ind])
        result = ""
        if ind != len(lines):
            result = '\n'.join(lines[ind + 1:])
        record = Query(sql=sql, result=result, data_type=data_type,
                       sort=sort_mode, label=label, id=self.record_id)
        return record

    def _parse_script_lines(self, lines: list):
        # Now the first line are exact command
        line_num = len(lines)
        if line_num == 0:
            return
        tokens = lines[0].split()
        record_type = tokens[0]
        record = Statement(id=self.record_id)

        if record_type == 'statement':
            # assert (line_num <= 2), 'statement too long: ' + '\n'.join(lines)
            status = (tokens[1] == 'ok')
            # Only a single SQL command is allowed per statement
            # r = Statement(sql=lines[1], status=status)
            record = Statement(sql="".join(lines[1:]), result=str(
                status), status=status, id=self.record_id)
            self.record_id += 1
        elif record_type == 'query':
            record = self.get_query(tokens=tokens, lines=lines)
            self.record_id += 1

        elif record_type == 'skipif':
            if tokens[1] in self.dbms_set:
                self.dbms_set.remove(tokens[1])
            else:
                pass
            record = self._parse_script_lines(lines[1:])
            if record:
                record.set_execute_db(self.dbms_set)
            # print(tmp_dbms_set)
        elif record_type == 'onlyif':
            if tokens[1] in DBMS_Set:
                tmp_dbms_set = set([tokens[1]])
            else:
                logging.debug(
                    "DBMS %s support is stll not implemented, skip this script", tokens[1])
                return
            record = self._parse_script_lines(lines[1:])
            if record:
                record.set_execute_db(tmp_dbms_set)
        elif record_type == 'hash-threshold':
            self.hash_threshold = eval(tokens[1])
            return
        elif record_type == 'halt':
            record = Control(action=RunnerAction.HALT,
                             id=self.record_id, sql=record_type)
            self.record_id += 1
        else:
            record = self.testfile_dialect_handler(
                lines=lines, record_type=record_type, id=self.record_id)
            if record:
                self.record_id += 1
                return record
            logging.warning("This script has not implement: %s", lines)
            return
        return record

    def parse_script(self, script: str):

        # Lines of the test script that begin with the sharp
        # character ("#", ASCII code 35) are comment lines and are ignored
        lines = [line for line in script.split('\n') if line[0] != '#']
        self.dbms_set = copy(DBMS_Set)
        r = self._parse_script_lines(lines)
        if r:
            self.records.append(r)

    # Each record is separated from its neighbors by one or more blank line.

    def parse_file(self):
        """ parse the file by double \\n into a list\n. Then call parse_script() 
        """
        self.scripts = [script.strip()
                        for script in self.test_content.strip().split('\n\n') if script != '']
        # print(self.scripts)
        self.records = []
        self.record_id = 0
        for _, script in enumerate(self.scripts):
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
            print('result:', rec.result)
            print('support db:', rec.db)
            print('status', rec.status)


class MYTParser(Parser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)
        self.testfile = filename
        self.resultfile = filename.replace('/t/', '/r/')

    def get_file_name(self, filename):
        self.testfile = filename
        self.resultfile = filename.replace('/t/', '/r/')

    def get_file_content(self):
        with open(self.testfile, 'r', encoding='utf-8') as testfile:
            self.test_content = testfile.read()

        with open(self.resultfile, 'r', encoding='utf-8') as testfile:
            self.result_content = testfile.read()


class DTParser(SLTParser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)

    def testfile_dialect_handler(self, *args, **kwargs):
        record_type = kwargs['record_type']
        lines = kwargs['lines']
        if record_type in ('loop', 'require'):
            logging.warning("This script has not implement: %s", lines)
            return Control(action=RunnerAction.HALT, id=self.record_id)
        return super().testfile_dialect_handler(*args, **kwargs)

    def parse_script(self, script: str):
        script = strip_comment_lines(script)
        if script:
            lines = script.split('\n')
        else:
            return
        print(self.filename, script)
        self.dbms_set = copy(DBMS_Set)

        tokens = lines[0].split()
        record_type = tokens[0]
        record = Statement(id=self.record_id)

        if record_type == 'statement':
            status = (tokens[1] == 'ok')
            statements = ("".join([strip_comment_suffix(line)
                          for line in lines[1:]])).strip().split(';\n')
            statements = list(filter(None, statements))
            for stmt in statements:
                record = Statement(sql=stmt, result=str(status), status=status, id=self.record_id)
                if stmt.split()[0].upper() == 'PRAGMA':
                    record.set_execute_db('duckdb')
                self.records.append(record)
                self.record_id += 1
        elif record_type == 'query':
            record = self.get_query(tokens=tokens, lines=lines)
            record.result = re.sub(r'true(\t|\n)', r'True\1', record.result)
            record.result = re.sub(r'false(\t|\n)', r'False\1', record.result)
            if record.result == 'true' or record.result == 'false':
                record.result = record.result.capitalize()
            record.set_resformat(ResultFormat.ROW_WISE)
            self.records.append(record)
            self.record_id += 1
        else:
            record = self.testfile_dialect_handler(
                lines=lines, record_type=record_type, id=self.record_id)
            if record:
                self.records.append(record)
                self.record_id += 1
