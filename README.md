# SQuaLity

SQuaLity is a tool that unifies test suites from different Database Management Systems (DBMS). It performs cross-compatibility testing by running test cases from one DBMS on another, helping identify compatibility issues between different SQL dialects.

## Getting Started

**Requirements:**

- Python >= 3.10
- Run `pip3 install -r requirements.txt` to install dependencies
- DBMS server setup

### Demo

The following commands will clone SQuaLity, install the required packages, and run a single test case (executing an SQL Logic Test (SLT) test case on DuckDB using the DuckDB Python connector). SQuaLity logs the execution status in `logs/debug.log` and outputs results to `output/duckdb_sqlite_debug_results.csv`.

```shell
git clone git@github.com:suyZhong/SQuaLity.git
cd SQuaLity
pip3 install -r requirements.txt
./demo.sh
```

### Install Original Test Suites

The test suites are stored in `$DBMS_suites` folders and are not included in this artifact. The following commands download the *latest* original test suites from their official repositories.

```shell
cd SQuaLity
./scripts/install_test.sh
```

Original test cases are distributed under different licenses. For more information, please refer to:
- [SQLite](https://www.sqlite.org/copyright.html) is in the public domain and does not require a license.
- [DuckDB](https://github.com/duckdb/duckdb/blob/main/LICENSE) is licensed under the MIT License.
- [PostgreSQL](https://www.postgresql.org/about/licence/) is licensed under the [PostgreSQL License](https://www.opensource.org/licenses/postgresql).
- [MySQL](https://github.com/mysql/mysql-server/blob/trunk/LICENSE) is licensed under version 2 of the GNU General Public License (GPLv2).


### Test Suites Used in the Paper

For reproducibility purposes, the test suites used in our paper should be downloaded from this [link](https://doi.org/10.5281/zenodo.13896444). Unzip the downloaded file and place the test suites in the corresponding folders:

```.
├── duckdb_suites
├── mysql_suites
├── postgresql_suites
├── sqlite_suites
```


## Run SQuaLity (For Reproducibility)

### Analyze Test Suites (RQ1, RQ2)

We use Python scripts to analyze the test suites.

First, extract test cases from the test suites into our unified format:

```shell
python3 scripts/extract_testcases.py -s all
```

Then analyze the test suites:

```
python3 scripts/analyze_test_cases.py -m $MODE -o $OUTPUT_DIR
```

The `MODE` parameter specifies the analysis mode:

- `length`: Count the lines of code (LOC) for each test case (RQ1)
- `dist`: Count the distribution of overall SQL statements (RQ2)
- `select`: Count the distribution of SELECT statements (RQ2)
- `join`: Count the distribution of JOIN statements (RQ2)

Example: `python3 scripts/analyze_test_cases.py -m length -o output`

**Note:** For RQ2, the analysis might take a long time due to SQLite's large test suite. We plan to parallelize the analysis in future versions.

### Execute Test Suites

The following command runs SQuaLity on a specific DBMS using a specific test suite. Results are stored in `output/$DBMS_$SUITE_filter_results.csv`.

```shell
python3 main.py --dbms $DBMS --s $SUITE [-f DB_NAME] --dump_all --filter --log INFO
```

**Example 1:** Run SQuaLity on DuckDB using the PostgreSQL test suite:

```shell
python3 main.py --dbms duckdb --s postgresql  -f output/testpgdb --dump_all --filter --log INFO
```

**Example 2:** Run MySQL on the same test suite (requires setting up a MySQL server and configuring the connection in `./config/config.json`):

```shell
python3 main.py --dbms mysql --s postgresql  -f output/testpgdb --dump_all --filter --log INFO
```

### Results Files

After running the test suites, results are stored in the `output` directory and logs are stored in the `logs` directory. 

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

The `*.log*` files contain detailed information about test cases that succeeded or failed. The `*.log.out*` files contain execution summaries of the test cases.

The `*results.csv` files contain the execution results for each test case. The `*logs.csv` files contain the SQL statements used to build schemas for failing test cases.

### Analyze Results (RQ3, RQ4)

We use Jupyter notebooks to analyze the test suite results. 

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

Execute the Jupyter notebooks to analyze the test suite results. Manual analysis is required to understand the compatibility issues between different SQL dialects. The analysis generally includes the following steps:

1. Load the test suite results.
2. Filter test cases that failed using regular expressions.
3. Sample the remaining failed test cases. Sampled test cases are stored in `output/$DBMS_$SUITE_sample_100.csv`.
4. Manually analyze the sampled failed test cases to identify compatibility issues between different SQL dialects:
    - For each sampled test case, analyze the error reason and update the `ERROR_REASON` column in the CSV file.
    - Summarize the compatibility issues in `data/$SUITE_suite_errors.csv`.
5. Export analysis statistics.

