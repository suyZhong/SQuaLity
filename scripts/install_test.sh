echo "Clearing previous tests"
rm -rf ./duckdb_tests
rm -rf ./postgresql_tests

# temporary dir
mkdir -p ./tmp
cd ./tmp

# download duckdb
git clone --depth 1 https://github.com/duckdb/duckdb.git
cp -r duckdb/test ../duckdb_tests

# download postgresql
git clone --depth 1 https://github.com/postgres/postgres.git
cp -r postgres/src/test ../postgresql_tests

echo "Cleaning up"
cd ..
rm -rf ./tmp