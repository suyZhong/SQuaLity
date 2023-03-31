import re

POSTGRESQL_FILTER = {
    'REGRESS': lambda x: re.search('regresslib', str(x['SQL'])) is not None,
    
    'SHOWTABLE': lambda x: re.match(r'\\d\+', str(x['SQL'])) is not None,
    
    'NOEXIST': lambda x: re.match(r'ERROR: .* does not exist', str(x['ERROR_MSG'])) is not None
    or re.match(r'ERROR: .* does not exist', str(x['ACTUAL_RESULT'])) is not None,
    
    'ALREADYEXIST': lambda x: re.match(r'ERROR: .* already exists', str(x['ERROR_MSG'])) is not None,
    
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
