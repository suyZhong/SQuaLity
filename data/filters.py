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
