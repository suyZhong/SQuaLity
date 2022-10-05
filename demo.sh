#!/bin/bash

mkdir logs
mkdir output
python3 main.py --dbms duckdb -t demo/sqlite_tests-index-orderby_nosort-10-slt_good_23.test -f output/temp.db --log DEBUG