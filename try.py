from ast import parse
from src import testparser

file = "demo/sqlite_tests-random-aggregates-slt_good_100.test"
# file = "demo/trashy.slt"
script = """query
SHOW CREATE TABLE t1
----
Table	Create Table
t1	CREATE TABLE `t1` (
  `c1` tinyint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`c1`)
) ENGINE=ENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""


# script = "query\nSHOW CREATE TABLE t1"
p = testparser.SLTParser(filename=file)


# p.parse_script(script=script)
p.print_records()
p.get_file_content()
p.parse_file()
# p.print_scripts()
p.print_records()
