#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <total_time> <option>"
    exit 1
fi

cd resources/postgres/
# Run a PostgreSQL instance
./bin/pg_ctl -D ./data stop
rm -rf ./data
./bin/initdb -D ./data
./bin/postgres -D ./data -p 10010 > /dev/null 2>&1 &
sleep 1
./bin/createuser -s postgres -p 10010
sleep 1
./bin/psql -p 10010 -U postgres -c "CREATE DATABASE test;" -h localhost
sleep 1

cd ../..

# start testing:
echo "Starting testing for postgresql"
bash -c "$(python3 scripts/run_coverage.py -s postgresql 1> logs/debug.log 2>&1)" 

sleep 1

# start querying sqlite
cd resources/postgres/

# start coverage
lcov --capture --rc lcov_branch_coverage=1 --directory . --output-file coverage.info 1>/dev/null 2>&1
genhtml coverage.info --rc lcov_branch_coverage=1  --output-directory coverage > coverage.log
cd ../..
sleep 5



bash -c "$(python3 scripts/run_coverage.py -s duckdb 1> logs/debug2.log 2>&1)"
bash -c "$(python3 scripts/run_coverage.py -s sqlite 1> logs/debug3.log 2>&1)"

cd resources/postgres/

lcov --capture --rc lcov_branch_coverage=1 --directory . --output-file coverage2.info 1>/dev/null 2>&1
genhtml coverage2.info --rc lcov_branch_coverage=1  --output-directory coverage2 > coverage2.log


sleep infinity