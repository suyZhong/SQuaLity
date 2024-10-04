FROM ubuntu:22.04

RUN apt-get update --yes
RUN env DEBIAN_FRONTEND=noninteractive apt-get install git autoconf gcc g++ make unzip wget tcl --yes --no-install-recommends
RUN apt-get install lcov libreadline-dev zlib1g-dev ca-certificates flex bison vim python3 cmake --yes --no-install-recommends

WORKDIR /app

RUN useradd -u 1000 -m test -d /home/test && chmod 777 /app
# switch user

# get the DBMSs
RUN mkdir resources
WORKDIR /app/resources

RUN git clone https://github.com/postgres/postgres.git && \
    cd postgres && \
    git checkout bc9993a549

WORKDIR /app/resources/postgres

RUN ./configure --enable-coverage --prefix /app/resources/postgres && \
    make && \
    make install

WORKDIR /app/resources

RUN wget https://www.sqlite.org/src/zip/d8cd6d49/SQLite-b2534d8d.zip && \
    unzip SQLite-b2534d8d.zip && \
    mv SQLite-b2534d8d sqlite

WORKDIR /app/resources/sqlite

RUN ./configure --prefix=/app/resources/sqlite --enable-all --enable-gcov && \
    sed -i 's/USE_GCOV = 0/USE_GCOV = 1/' Makefile && \
    make 

WORKDIR /app/resources


RUN apt-get install python3-pip --yes
COPY requirements.txt /app/

RUN pip3 install -r /app/requirements.txt


RUN git clone https://github.com/duckdb/duckdb.git && \
    cd duckdb && \
    git checkout v0.8.1

WORKDIR /app/resources/duckdb

RUN ls

RUN mkdir build && mkdir build/coverage

WORKDIR /app/resources/duckdb/build/coverage

RUN pip3 install setuptools

RUN ls ../../ && cmake -E env CXXFLAGS="--coverage" cmake -DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0 -DCMAKE_BUILD_TYPE=Debug ../.. && cmake --build . -j 16


WORKDIR /app



RUN chown -R test /app

USER test
RUN mkdir logs/ && chmod 777 logs/


COPY sqlite_tests /app/sqlite_tests
COPY scripts /app/scripts
COPY duckdb_tests /app/duckdb_tests
COPY postgresql_tests /app/postgresql_tests
COPY config/ /app/config/
COPY src /app/src



RUN ls /app/postgresql_tests/

CMD ["sleep", "infinity"]
