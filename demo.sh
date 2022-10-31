#!/bin/bash

mkdir logs -p
mkdir output -p
./main.py --dbms duckdb -t demo/sqlite_tests-index-orderby_nosort-10-slt_good_23.test -f output/temp.db --dump_all --log DEBUG
