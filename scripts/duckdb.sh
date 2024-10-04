#!/bin/bash


# start testing:
echo "Starting testing for duckdb"
cd /app/resources/duckdb

lcov --zerocounters --directory .
lcov --capture --initial --directory . --base-directory . --no-external --output-file coverage.info

cd ../../

bash -c "$(python3 scripts/run_coverage_cli.py -s duckdb --cli /app/resources/duckdb/build/coverage/duckdb 1> logs/debug.log 2>&1)" 

sleep 1


cd /app/resources/duckdb
# start coverage
lcov --rc lcov_branch_coverage=1 --directory . --base-directory . --no-external --capture --output-file coverage.info
lcov --rc lcov_branch_coverage=1 --remove coverage.info $(< .github/workflows/lcov_exclude) -o lcov.info

sleep 5

cd ../../

bash -c "$(python3 scripts/run_coverage_cli.py -s postgresql --cli /app/resources/duckdb/build/coverage/duckdb 1> logs/debug3.log 2>&1)"

cd /app/resources/duckdb

lcov --rc lcov_branch_coverage=1 --directory . --base-directory . --no-external --capture --output-file coverage.info
lcov --rc lcov_branch_coverage=1 --remove coverage.info $(< .github/workflows/lcov_exclude) -o lcov.info

cd ../../

bash -c "$(python3 scripts/run_coverage_cli.py -s sqlite --cli /app/resources/duckdb/build/coverage/duckdb 1> logs/debug2.log 2>&1)"

cd /app/resources/duckdb

lcov --rc lcov_branch_coverage=1 --directory . --base-directory . --no-external --capture --output-file coverage.info
lcov --rc lcov_branch_coverage=1 --remove coverage.info $(< .github/workflows/lcov_exclude) -o lcov.info


sleep infinity