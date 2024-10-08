import re
import difflib
import sqlparse
from copy import copy
import pandas as pd
from .utils import *
from typing import List


def strip_hash_comment_lines(code: str):
    return re.sub(r'(?m)^ *#.*\n?', '', code)


def strip_dash_comment_lines(code: str):
    return re.sub(r'(?m)^ *--.*\n?', '', code)


def strip_comment_suffix(code: str):
    return re.sub(r'-- .*', '', code)


class Parser:
    def __init__(self, filename='') -> None:
        self.filename: str = filename
        self.test_content = ""
        self.result_content = ""
        self.hash_threshold = 8
        self.records: List[Record] = list()
        self.setup_records: List[Record] = list()
        self.record_id = 0

    def get_setup_tests(self):
        """get the environment set up records
        """

    def _read_file(self, filename):
        content = ""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(filename, 'r', encoding='windows-1252') as f:
                content = f.read()
        return content

    def _read_file_lines(self, filename):
        content = ""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.readlines()
        except UnicodeDecodeError:
            with open(filename, 'r', encoding='windows-1252') as f:
                content = f.readlines()
        return content

    def read_file(self, filename, byline=False):
        if byline:
            return self._read_file_lines(filename)
        else:
            return self._read_file(filename)

    def get_file_name(self, filename):
        self.filename = filename

    def get_file_content(self):
        self.test_content = self._read_file(self.filename)

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

    def debug(self):
        # my_debug(self.test_content)
        # my_debug(self.result_content)
        for record in self.records:
            my_debug(type(record))
            my_debug(record.sql)
            my_debug(record.result)
        # print(self.test_content)


class CSVParser(Parser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)
        self.compression = None

    def get_file_name(self, filename):
        self.compression = 'zip' if filename.endswith('.zip') else None
        self.filename = filename

    def get_file_content(self):
        try:
            self.test_content = pd.read_csv(
                self.filename, compression=self.compression, na_filter=False).fillna('')
        except FileNotFoundError:
            self.test_content = pd.DataFrame([])
            logging.warning("Test file not find or not in the correct form!")

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
        self.resultfile = filename.replace(
            '/t/', '/r/').replace('.test', '.result')
        self.delimiter = ';'

    def get_file_name(self, filename):
        self.testfile = filename
        self.resultfile = filename.replace(
            '/t/', '/r/').replace('.test', '.result')

    def get_file_content(self):
        self.test_content = self._read_file(self.testfile)
        self.result_content = self._read_file(self.resultfile)

    def find_next_command(self, id):
        if id + 1 >= len(self.records):
            return ""
        command = self.records[id + 1].sql
        if command != "":
            return command.split('\n')[0]
        else:
            return self.find_next_command(id + 1)

    def get_test_commands(self):
        record_id = 0
        command = []
        for i, script in enumerate(self.scripts):
            if script.startswith('#'):
                continue
            if script.startswith('--'):
                tokens = script.split()
                action = RunnerAction[tokens[0][2:].upper()]
                self.records.append(Control(sql=' '.join(
                    tokens[1:]), action=action, id=record_id))
                record_id += 1
            else:
                command.append(script)
                if script.endswith(self.delimiter):
                    self.records.append(
                        Record(sql="\n".join(command), id=record_id))
                    command = []
                    record_id += 1

    def get_test_results(self):
        for i, record in enumerate(self.records):
            if type(record) is Record:
                command = record.sql.split('\n')[-1]
                assert i == record.id
                # print(record.id, len(self.records))
                next_command = self.find_next_command(record.id)
                loc = self.result_content.find(command) + len(command) + 1
                self.result_content = self.result_content[loc:]
                next_loc = self.result_content.find(
                    next_command) if next_command != "" else 0
                if next_loc > 0:
                    result = self.result_content[:next_loc]
                else:
                    result = ""
                record.result = result

    def parse_file(self):
        self.scripts = [script.strip() for script in self.test_content.strip().split(
            '\n') if script != '']

        # parse the test file and get commands
        self.get_test_commands()

        # parse the result file and get results
        self.get_test_results()


class PGTParser(Parser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)
        self.testfile = filename
        self.resultfile = filename.replace(
            '/sql/', '/expected/').replace('.sql', '.out')
        self.delimiter = ';'
        self.meta_data = {'psql_testcase': 0, 'total_files': 0,
                          'total_testcase': 0, 'psql_files': 0}
        self.get_setup_tests(filename)

    def get_setup_tests(self, filename):
        if filename != '':
            self.get_file_name(filename)
            self.get_file_content()
            self.parse_file()
            self.setup_records = copy(self.records)

    def get_file_name(self, filename: str):
        self.testfile = filename
        self.resultfile = filename.replace(
            '/sql/', '/expected/').replace('.sql', '.out')

    def get_file_content(self):
        self.meta_data['total_files'] += 1
        self.test_content = self._read_file(self.testfile)
        self.result_content = self._read_file(self.resultfile)
        self.test_content = re.sub(
            r"; {1,2}--.*?$", ";", self.test_content, flags=re.MULTILINE)
        self.result_content = re.sub(
            r"; {1,2}--.*?$", ";", self.result_content, flags=re.MULTILINE)

    def testfile_dialect_handler(self, *args, **kwargs):

        return super().testfile_dialect_handler(*args, **kwargs)

    def get_diff(self):
        test_differ = difflib.Differ()
        test_lines = [line for line in self.test_content.splitlines(
            keepends=True) if line != '\n']
        result_lines = [line for line in self.result_content.splitlines(
            keepends=True) if line != '\n']
        return list(test_differ.compare(test_lines, result_lines))

    def get_merge(self):
        test_lines = [line for line in self.test_content.splitlines(
            keepends=True) if line != '\n']
        result_lines = [line for line in self.result_content.splitlines(
            keepends=True) if line != '\n']

    def split_file(self):
        test_content = self.test_content
        test_content = ''.join([line if not line.startswith(
            '\\') and not line.endswith('\\gset\n') else line.strip('; \n') + ';\n' for line in self.test_content.splitlines(keepends=True)])
        commands = sqlparse.split(test_content)
        # currently do a hack here
        if self.testfile.endswith('gin.sql'):
            commands = commands[:-1] + [command.strip(
                ';\n') + ';\n' for command in commands[-1].split(';\n')]

        # we need to do a special handling for the case that we have \if \endif psql commands
        for i in range(0, len(commands)):
            if commands[i] == "\\endif;":
                commands[i - 2] = "".join(commands[i - 2: i + 1]
                                          ).replace(';', '\n')
                commands.pop(i - 1)
                commands.pop(i - 1)
                break
        return commands

    def parse_file_by_split(self):
        commands = self.split_file()

        # clear the empty line in the commands
        commands = ['\n'.join(
            [line for line in command.splitlines() if line]) for command in commands if command != ';']
        pure_commands = [strip_dash_comment_lines(
            command) for command in commands]

        result_lines = [
            line for line in self.result_content.splitlines() if line != '']

        # Compare with the commands parsed by sqlparse
        # split the diff according to the line number
        ind = 0
        num_command = len(commands)
        num_input = 0
        psql_flag = False
        for i, command in enumerate(commands):
            result = ""
            command_lines = [line.strip(';') for line in command.splitlines()]
            # if next command is actually a input, skip this one
            for k in range(i, num_command):
                next_command = commands[k + 1] if k < num_command - 1 else "\n"
                if not next_command.endswith('\\.;'):
                    break
            next_line = next_command.splitlines()[0].strip(
                ';') if next_command != ';' else ''

            if command_lines[0] == result_lines[ind].strip(';'):
                ind += len(command.split('\n'))
            # skip the input command
            else:
                self.records[i - 1 - num_input].input_data = command.strip(';')
                num_input += 1
                continue

            # boundary checking
            if next_line == "":
                result = '\n'.join(result_lines[ind: len(result_lines)])
            elif result_lines[ind].strip(';') == next_line:
                result = ""
                # there would be something like this:
                # --
                # (1 row)
                # Which would cause comment line to match --
                if next_line == "--":
                    if result_lines[ind + 1].strip(';') == "(1 row)":
                        result = "--\n(1 row)\n"
                        ind += 2
            else:
                for j in range(ind, len(result_lines)):
                    if result_lines[j] .strip(';') == next_line:
                        result = '\n'.join(result_lines[ind:j])
                        ind = j
                        break
            # Create new record
            self.meta_data['total_testcase'] += 1
            if re.match(r'^[\\]', command.strip()):
                # logging.warning('Currently not support psql commands like {}, change to HALT'.format(command))
                self.meta_data['psql_testcase'] += 1
                if re.match(r'^[\\]quit', command.strip()):
                    self.records.append(
                        Control(id=i - num_input, sql=command.strip(';'), result=result))
                else:
                    self.records.append(
                        Record(id=i - num_input, sql=command.strip(';'), result=result))
                psql_flag = True
                # print("psql:", command)
                # break
            else:
                self.records.append(
                    Record(sql=strip_comment_suffix(pure_commands[i]).strip(';'), id=i - num_input, result=result))
        if psql_flag:
            self.meta_data['psql_files'] += 1

    def convert_record(self, record: Record):
        '''
        The function convert the expected result in postgres to the more general form in SQuaLity.
        '''
        # if record is control, skip
        converted_result, is_query = convert_postgres_result(record.result)
        if type(record) == Control:
            record.result = converted_result
            return record
        converted_record = Statement(id=record.id)
        # Statement ok
        if converted_result == "" and not is_query:
            return Statement(sql=record.sql, id=record.id, input_data=record.input_data)
        # Statement error
        elif converted_result.startswith("ERROR"):
            return Statement(sql=record.sql, result=converted_result, status=False, id=record.id, input_data=record.input_data)
        # Query
        elif converted_result or is_query:
            data_type = 'I' * len(converted_result.split('\n')[0].split('\t'))
            return Query(sql=record.sql, result=converted_result, id=record.id, res_format=ResultFormat.ROW_WISE, data_type=data_type, input_data=record.input_data)
        logging.warning("Unknown result: %s", converted_result)
        return converted_record

    def convert_records(self):
        for i, record in enumerate(self.records):
            self.records[i] = self.convert_record(record)

    def parse_file(self):
        self.records = []

        # self.parse_file_by_differ()
        self.parse_file_by_split()
        self.convert_records()


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

    def get_file_content(self):
        return super().get_file_content()

    def parse_script(self, script: str):
        script = strip_hash_comment_lines(script)
        if script:
            lines = script.split('\n')
        else:
            return
        # print(self.filename, script)
        self.dbms_set = copy(DBMS_Set)

        tokens = lines[0].split()
        record_type = tokens[0]
        record = Statement(id=self.record_id)

        if record_type == 'statement':
            # if there is connection
            if len(tokens) > 2:
                return
            status = (tokens[1] == 'ok')
            if not status and any([line == '----' for line in lines[1:]]):
                lines = lines[:lines.index('----')]
            # We need to split the statement into several statements
            # some of the DuckDB test case are like:
            # statement ok
            # CREATE .......;
            # INSERT .........;
            statements = (" ".join([re.sub(r'--.*', '', line)
                          for line in lines[1:]])).strip().split('; ')
            statements = list(filter(None, statements))
            for stmt in statements:
                record = Statement(sql=stmt.rstrip(';'), result=str(
                    status), status=status, id=self.record_id, suite='duckdb')
                # if stmt.split()[0].upper() == 'PRAGMA':
                #     record.set_execute_db({'duckdb'})
                self.records.append(record)
                self.record_id += 1
        elif record_type == 'query':
            # if there is connection DuckDB do a dirty hack
            if len(tokens) >= 3:
                if tokens[2] not in ['nosort', 'rowsort', 'valuesort']:
                    return
            
            record = self.get_query(tokens=tokens, lines=lines)
            record.suite = 'duckdb'
            record.is_hash = False

            # In DuckDB implementation they do like this. A dirty way.
            # https://github.com/duckdb/duckdb/blob/master/test/sqlite/result_helper.cpp#L391
            if record.result.find("values hashing to") > 0:
                record.set_resformat(ResultFormat.HASH)
                my_debug("hash")
                record.is_hash = True
                self.records.append(record)
                self.record_id += 1
            elif record.result.find('<REGEX>') >= 0 or record.result.find('<!REGEX>') >= 0:
                my_debug("regex")
                record.set_resformat(ResultFormat.REGEX)
                self.records.append(record)
                self.record_id += 1
            else:
                cols = len(record.data_type)
                # If the result has a sort type, then it is value wise
                if record.sort == SortType.NO_SORT:
                    if cols > 1:
                        result_lines = record.result.split('\n')
                        # If DuckDB make the result value wise, convert it to row wise
                        if result_lines[0].find('\t') < 0:
                            # change the value wise into row wise
                            # First split the result_lines into cols chunks
                            # Then join them together by "\t" and then by "\n"
                            record.result = '\n'.join(['\t'.join(row) for row in [
                                                      result_lines[i:i+cols] for i in range(0, len(result_lines), cols)]])
                        # remove the redundant \t
                        if result_lines[0].count('\t') >= cols:
                            record.result = '\n'.join(
                                [re.sub('\t+', '\t', line) for line in result_lines])
                    record.set_resformat(ResultFormat.ROW_WISE)
                # Not quite exact. DuckDB use a guess to determine the row-wise...
                if cols > 1:
                    result_lines = record.result.split('\n')
                    if all([len(line.split('\t')) == cols for line in result_lines]):
                        record.set_resformat(ResultFormat.ROW_WISE)
                # Some are not value-wise and the result is not normal object. So we need to convert it to row-wise
                if record.data_type == 'I':
                    record.set_resformat(ResultFormat.ROW_WISE)

                record.result = re.sub(
                    r'true(\t|\n|$)', r'True\1', record.result)
                record.result = re.sub(
                    r'false(\t|\n|$)', r'False\1', record.result)
                # record.result = record.result.replace('(empty)', '')
                # if record.result == 'true' or record.result == 'false':
                #     record.result = record.result.capitalize()
                self.records.append(record)
                self.record_id += 1
        else:
            record = self.testfile_dialect_handler(
                lines=lines, record_type=record_type, id=self.record_id)
            if record:
                record.suite = 'duckdb'
                self.records.append(record)
                self.record_id += 1


class CDBTParser(SLTParser):
    def __init__(self, filename='') -> None:
        super().__init__(filename)

    def testfile_dialect_handler(self, *args, **kwargs):
        record_type = kwargs['record_type']
        lines = kwargs['lines']
        if record_type in ('loop', 'require', 'user', ):
            logging.warning("This script has not implement: %s", lines)
            return Control(action=RunnerAction.HALT, id=self.record_id)
        return super().testfile_dialect_handler(*args, **kwargs)

    def get_query(self, tokens, lines):

        return super().get_query(tokens, lines)

    def parse_script(self, script: str):
        script = strip_hash_comment_lines(script)
        if script:
            lines = script.split('\n')
        else:
            return
        self.dbms_set = set(['cockroach'])

        tokens = lines[0].split()
        record_type = tokens[0]
        record = Statement(id=self.record_id)

        if record_type == 'statement':
            status = (tokens[1] == 'ok') if len(tokens) > 1 else True
            statements = ("".join([strip_comment_suffix(line)
                          for line in lines[1:]])).strip().split(';\n')
            statements = list(filter(None, statements))
            for stmt in statements:
                record = Statement(sql=stmt, result=" ".join(
                    tokens[2:]), status=status, id=self.record_id)
                self.records.append(record)
                self.record_id += 1
        elif record_type == 'query':
            record = self.get_query(tokens=tokens, lines=lines)

            record.set_resformat(ResultFormat.ROW_WISE)
            self.records.append(record)
            self.record_id += 1
        else:
            record = self.testfile_dialect_handler(
                lines=lines, record_type=record_type, id=self.record_id)
            if record:
                self.records.append(record)
                self.record_id += 1
