import re
from collections import defaultdict

ERR_MSGS = ['SYNTAX', 'NOEXIST', 'ALREADYEXIST']

SQLITE_MSG = {
    'SYNTAX': re.compile(r'near ".*": syntax error|unrecognized token: ".*"'),
    'NOEXIST': re.compile(r'no such .*: .*'),
    'ALREADYEXIST': re.compile(r'(?i)table .* already exists'),
}

DUCKDB_MSG = {
    'SYNTAX': re.compile(r'(?i)near ".*": syntax error'),
}

MYSQL_MSG = {
    'SYNTAX': re.compile(r'1064 \(42000\)'),
    'NOEXIST': re.compile(r'1146 \(42S02\)'),
    'ALREADYEXIST': re.compile(r'1050 \(42S01\)'),
    'RUNNER_COMMAND': re.compile("|".join(["connection", "query", "connect", "sleep", "inc", "dec", "source",
                                           "disconnect", "let", "echo", "expr", "while", "end", "save_master_pos",
                                           "sync_with_master", "sync_slave_with_master", "error", "send", "reap",
                                           "dirty_close", "replace_result", "replace_column", "ping", "eval",
                                           "enable_query_log", "disable_query_log",
                                           "enable_result_log", "disable_result_log", "enable_connect_log",
                                           "disable_connect_log", "wait_for_slave_to_stop", "enable_warnings",
                                           "disable_warnings", "enable_info", "disable_info",
                                           "enable_session_track_info", "disable_session_track_info",
                                           "enable_metadata", "disable_metadata", "enable_async_client",
                                           "disable_async_client", "exec", "execw", "exec_in_background", "delimiter",
                                           "disable_abort_on_error", "enable_abort_on_error", "vertical_results",
                                           "horizontal_results", "query_vertical", "query_horizontal", "sorted_result",
                                           "partially_sorted_result", "lowercase_result", "skip_if_hypergraph",
                                           "start_timer", "end_timer", "character_set", "disable_ps_protocol",
                                           "enable_ps_protocol", "disable_reconnect", "enable_reconnect", "if",
                                           "disable_testcase", "enable_testcase", "replace_regex",
                                           "replace_numeric_round", "remove_file", "file_exists", "write_file",
                                           "copy_file", "perl", "die", "assert",
                                           "exit", "skip", "chmod", "append_file", "cat_file", "diff_files",
                                           "send_quit", "change_user", "mkdir", "rmdir", "force-rmdir", "force-cpdir",
                                           "list_files", "list_files_write_file", "list_files_append_file",
                                           "send_shutdown", "shutdown_server", "result_format", "move_file",
                                           "remove_files_wildcard", "copy_files_wildcard", "send_eval", "output",
                                           "reset_connection", "query_attributes", ])),
}

COCKROACHDB_MSG = {
    
}

POSTGRESQL_MSG = {
    'SYNTAX': re.compile(r'(?i)near ".*": syntax error'),
    'NOEXIST': re.compile(r'ERROR: .* does not exist'),
    'ALREADYEXIST': re.compile(r'ERROR: .* already exists'),
}

DBMS_MSG_MAP = {
    'sqlite': SQLITE_MSG,
    'duckdb': DUCKDB_MSG,
    'mysql': MYSQL_MSG,
    'cockroachdb': COCKROACHDB_MSG,
    'postgresql': POSTGRESQL_MSG,
    'psql': POSTGRESQL_MSG,
}

POSTGRESQL_FILTER = {
    # SQL pattern related filters
    'REGRESS': lambda x: re.search('regresslib', str(x['SQL'])) is not None,
    'SHOWTABLE': lambda x: re.match(r'\\d\+', str(x['SQL'])) is not None,
    
    # ERROR_MSG related filters
    
    'NOEXIST': lambda x: re.match(DBMS_MSG_MAP[x['DBMS_NAME']]['NOEXIST'], str(x['ERROR_MSG'])) is not None
    or re.match(DBMS_MSG_MAP[x['DBMS_NAME']]['NOEXIST'], str(x['ACTUAL_RESULT'])) is not None,
    
    'ALREADYEXIST': lambda x: re.match(DBMS_MSG_MAP[x['DBMS_NAME']]['ALREADYEXIST'], str(x['ERROR_MSG'])) is not None,
    'SYNTAX': lambda x: re.search(DBMS_MSG_MAP[x['DBMS_NAME']]['SYNTAX'], str(x['ERROR_MSG'])) is not None,
    
    # Test case specific filters
    'LARGEOBJ': lambda x: x['TESTFILE_PATH'] in ['postgresql_tests/regress/sql/largeobject.sql', ],
    
    'COLLATION': lambda x: x['TESTFILE_PATH'] in ['postgresql_tests/regress/sql/char.sql',
                                                     'postgresql_tests/regress/sql/varchar.sql',
                                                  'postgresql_tests/regress/sql/select_implicit.sql',
                                                  'postgresql_tests/regress/sql/tsearch.sql',
                                                  ],
    
    'TIMESTAMP': lambda x: x['TESTFILE_PATH'] in ['postgresql_tests/regress/sql/horology.sql', 
                                                  'postgresql_tests/regress/sql/timestamp.sql',
                                                  'postgresql_tests/regress/sql/timestamptz.sql',
                                                  'postgresql_tests/regress/sql/window.sql',
                                                  ],
    
    'FUNCFORMAT': lambda x: x['TESTFILE_PATH'] in ['postgresql_tests/regress/sql/numeric.sql', ],
    
    'PREPARETXN': lambda x: x['TESTFILE_PATH'] in ['postgresql_tests/regress/sql/prepared_xacts.sql', ],
}

MYSQL_FILTER = {
    'RC': lambda x: re.match(MYSQL_MSG['RUNNER_COMMAND'], x) is not None,
    'DELIMETER': lambda x: re.match(r'(?i)DELIMETER', x) is not None,
}
