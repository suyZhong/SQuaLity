echo "Clearing previous tests"
rm -rf ./duckdb_tests
rm -rf ./postgresql_tests
rm -rf ./sqlite_tests
rm -rf ./mysql_tests

# temporary dir
mkdir -p ./tmp
cd ./tmp

# download duckdb
echo "Downloading DuckDB tests"
git clone --depth 1 https://github.com/duckdb/duckdb.git
cp -r duckdb/test ../duckdb_tests

# download postgresql
echo "Downloading PostgreSQL tests"
git clone --depth 1 https://github.com/postgres/postgres.git
cp -r postgres/src/test ../postgresql_tests

# download sqllogictest
echo "Downloading SQL Logic Test"
git clone --depth 1 https://github.com/gregrahn/sqllogictest.git
cp -r sqllogictest/test ../sqlite_tests

# download mysql test
echo "Downloading MySQL tests"
git clone --depth 1 https://github.com/mysql/mysql-server.git
cp -r mysql-server/mysql-test ../mysql_tests

echo "Cleaning up"
cd ..
rm -rf ./tmp
