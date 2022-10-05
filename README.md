# CS6218_SQuaLity

## Environments

```shell
pip install -r requirements.txt
```

## Usage

```shell
# Run a sqllogictest file using duckdb 
python main.py --dbms duckdb -t demo/sqlite_tests-index-orderby_nosort-10-slt_good_23.test -f output/temp.db --log DEBUG
```