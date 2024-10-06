# SQuaLity

SQuaLity is a tool that unified several test suites from different Database Management Systems (DBMS). It can do a cross check for one DBMS using test cases of others, and find compatibility issues of different SQL dialects.

## Getting started

Requirements:

- python>=3.9
- Run `pip3 install -r requirements.txt` in command line
- MySQL, PostgreSQL and CockroachDB server setup

### demo

The following commands clone SQuaLity, install the packages and run a single test case (by running a SQL Logic Test (SLT) test case on DuckDB using DuckDB Python connector). SQuaLity would log the running status in `logs/debug.log` and output the results in `output/duckdb_sqlite_debug_results.csv`.

```shell
git clone git@github.com:suyZhong/SQuaLity.git
cd SQuaLity
pip3 install -r requirements.txt
./demo.sh
```

### Install original test suites

The test suites are stored in `$DBMS_suites` folders. The following commands download the *latest* original test suites from the official repositories.

```shell
cd SQuaLity
./scripts/install_test.sh
```

## Run SQuaLity

### Analyze test suites (RQ1, RQ2)

We use python scripts to analyze the test suites. 

First, extract the test cases from the test suites to our unified format.

```shell
python3 scripts/extract_test_cases.py -s all
```

Then, analyze the test suites.

```
python3 scripts/analyze_test_cases.py -m $MODE -o $OUTPUT_DIR
```

The `MODE` specifies the analysis mode.

- `length`: count the LOC of each test case (RQ1)
- `dist`: count the distribution of the overall SQL statements (RQ2)
- `select`: count the distribution of the SELECT statements (RQ2)
- `join`: count the distribution of the JOIN statements (RQ2)

Note: for RQ2, might take a long time to run the analysis due to SQLite's large test suite. Will parrelize the analysis in the future.

### Execute test suites

The following command runs SQuaLity on a specific DBMS using a specific test suite. The results are stored in `output/$DBMS_$SUITE_results.csv`.

```shell
python3 main.py --dbms $DBMS --s $SUITE [-f DB_NAME] --dump_all --filter --log INFO
```

For example, run SQuaLity on DuckDB using the PostgreSQL test suite:

```shell
python3 main.py --dbms duckdb --s postgresql  -f output/testpgdb --dump_all --filter --log INFO
```

Or run MySQL on it (create a MySQL server first and set up the connection in `./config/config.json`):

```shell
python3 main.py --dbms mysql --s postgresql  -f output/testpgdb --dump_all --filter --log INFO
```

### Results files

After running the test suites, the results are stored in `output` and the logs are stored in `logs`. 

```
.
├── logs
│   ├── $DBMS_$SUITE-$date.log
│   ├── $DBMS_$SUITE-$date.log.out
├── output
│   ├── $DBMS_$SUITE_filter_logs.csv
│   ├── $DBMS_$SUITE_filter_results.csv
...
```

The `*.log*` file contains the detailed information of the test cases that successed or failed. The `*.log.out*` file contains the running summary of the test cases.

The `*results.csv` file contains the execution result of each test case. The `*logs.csv` file contains the SQL statements that built the schema of failing test cases.

### Analyze results (RQ3, RQ4)

We use jupyter notebook to analyze the results of the test suites. 

```shell
.
├── scripts
│   ├── RQ3-SampleDuckDB.ipynb
│   ├── RQ3-SamplePostgres.ipynb
│   ├── RQ4-BugAnalysisDuckDBTest.ipynb
│   ├── RQ4-BugAnalysisPGTest.ipynb
│   ├── RQ4-BugAnalysisSLT.ipynb
...
```

Execute the jupyter notebook to analyze the results of the test suites. Manual analysis is required to understand the compatibility issues of different SQL dialects. In general, the analysis includes the following steps:

1. Load the results of the test suites.
2. Filter some test cases that failed in the test suites by regular expressions.
3. Sample the rest of the failed test cases. Sampled test cases are stored in `output/$DBMS_$SUITE_sample_100.csv`.
4. Manully analyze the sampled failed test cases and find the compatibility issues of different SQL dialects.
    - For each of the sampled test case, we analyze the error reason and update the column `ERROR_REASON` in the csv file.
    - We summarize the compatibility issues in `data/$SUITE_suite_errors.csv`.
5. Export the analysis statistics.

