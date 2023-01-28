#!/bin/bash

set -e
set -x

tables=$1
table_size=$2
threads=$3

if [ "$#" -ne 3 ]; then
    echo "Usage: ./prepare_tables.sh <tables> <table_size> <threads>" >&2
      exit 1
fi

/home/khuang/local/pgsql/bin/psql $USER -c "DROP DATABASE IF EXISTS dbtest;"
/home/khuang/local/pgsql/bin/psql $USER -c "CREATE DATABASE dbtest;"

sysbench ./sysbench/oltp_common_pgsql.lua --create_secondary=off --tables=$tables --pgsql-db=dbtest --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=5432 --pgsql-user=$USER --threads=1 create

sysbench ./sysbench/oltp_common_pgsql.lua --tables=$tables --table_size=$table_size --pgsql-db=dbtest --db-driver=pgsql --pgsql-host=127.0.0.1 --pgsql-port=5432 --pgsql-user=$USER --threads=$threads prepare
