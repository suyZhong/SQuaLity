# SQuaLity

SQuaLity is a tool that unified several test suites from different Database Management Systems (DBMS). It can do a cross check for one DBMS using test cases of others, and find compatibility issues of different SQL dialects.

# Getting started

Requirements:

- python>=3.9
- Run `pip3 install -r requirements.txt` in command line
- MySQL, PostgreSQL and CockroachDB server setup

The following commands clone SQuaLity, install the packages and run a single test case (by running a SQL Logic Test (SLT) test case on DuckDB using DuckDB Python connector). SQuaLity would log the running status in `logs/debug.log` and output the results in `output/duckdb_logs.csv`.

```shell
git clone git@github.com:suyZhong/SQuaLity.git
cd SQuaLity
pip3 install -r requirements.txt
./demo.sh
```

