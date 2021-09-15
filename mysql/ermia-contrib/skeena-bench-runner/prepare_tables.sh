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

mysql << EOF
DROP DATABASE IF EXISTS dbtest;
CREATE DATABASE dbtest;
EOF

sysbench ./sysbench/oltp_common_cross.lua --mysql-ignore-errors=1180,1030,1412 --mysql_storage_engine=ERMIA --create_secondary=off --tables=$tables --mysql-db=dbtest --db-driver=mysql --mysql-socket=/dev/shm/$USER/mysql/mysqld.sock --threads=1 create

sysbench ./sysbench/oltp_common_cross.lua --mysql-ignore-errors=1180,1030,1412 --mysql_storage_engine=ERMIA --tables=$tables --table_size=$table_size --mysql-db=dbtest --db-driver=mysql --mysql-socket=/dev/shm/$USER/mysql/mysqld.sock --threads=$threads prepare
