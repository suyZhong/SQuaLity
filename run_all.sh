#!/bin/bash

# Extract test cases TODO  ---- For Section 3.1 in PAper
echo "Extract Test cases (TODO)"

# ./extract_testcase.py

# Run SLT test cases on DBMSs and save bugs ---- For Section 3.2 in Paper

for dbms in 'mysql' 'cockroachdb' 'postgresql'
do
    echo "Testing $dbms"
    echo "Start docker container"
    docker start $dbms-test
    sleep 1
    echo "python3 main.py --dbms $dbms -f tempdb --log INFO >> logs/system_$dbms.log"
    python3 main.py --dbms $dbms -f tempdb --log INFO >> logs/system_$dbms.log
    docker stop $dbms-test

done

for dbms in 'sqlite' 'duckdb'
do
    echo "Testing $dbms"
    echo "python3 main.py --dbms $dbms -f tempdb --log INFO >> logs/system_$dbms.log"
    python3 main.py --dbms $dbms -f tempdb --log INFO >> logs/system_$dbms.log
done

# Analyze the Test results and output to the tex/figures ---- For Section 3.3 paper
echo "Analyze the test results (TODO)"

# ./generate_tex.py
# ./generate_fig.py