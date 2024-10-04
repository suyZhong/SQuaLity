#!/bin/bash


# start testing:
echo "Starting testing for sqlite"
bash -c "$(python3 scripts/run_coverage_cli.py -s sqlite --cli /app/resources/sqlite/sqlite3 1> logs/debug.log 2>&1)" 

sleep 1

# start querying sqlite
cd resources/sqlite/
# start coverage
lcov --capture --rc lcov_branch_coverage=1 --directory . --output-file coverage.info 1>/dev/null 2>&1
genhtml coverage.info --rc lcov_branch_coverage=1  --output-directory coverage > coverage.log
cd ../../
sleep 5
bash -c "$(python3 scripts/run_coverage_cli.py -s duckdb --cli /app/resources/sqlite/sqlite3 1> logs/debug2.log 2>&1)"
bash -c "$(python3 scripts/run_coverage_cli.py -s postgresql --cli /app/resources/sqlite/sqlite3 1> logs/debug3.log 2>&1)"

cd resources/sqlite/

lcov --capture --rc lcov_branch_coverage=1 --directory . --output-file coverage2.info 1>/dev/null 2>&1
genhtml coverage2.info --rc lcov_branch_coverage=1  --output-directory coverage2 > coverage2.log
cd ../..

sleep infinity