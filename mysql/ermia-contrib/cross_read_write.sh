#!/bin/bash
set -x
set -e

tables=$1
table_size=$2
n_connections=$3
p_innodb=$4
duration=$5

if [ "$#" -ne 5 ]; then
  echo "Usage: ./cross_read_write.sh <tables> <table_size> <connections> <innodb%> <duration>" >&2
  exit 1
fi

if [ $p_innodb -lt 0 ] || [ $p_innodb -gt 100 ]; then
    echo "innodb% should have a value between 0 and 100." >&2
      exit 1
fi

# Read write workload
taskset -c <client threads> sysbench ./sysbench/oltp_read_write_cross.lua \
--mysql-ignore-errors=1180,1030,1412,1213 \
--innodb_percentage=$p_innodb \
--create_secondary=off \
--rand-type=uniform \
--tables=$tables \
--range_selects=off \
--table_size=$table_size \
--mysql_storage_engine=ERMIA \
--mysql-db=dbtest \
--db-driver=mysql \
--mysql-socket=/dev/shm/$USER/mysql/mysqld.sock \
--threads=$n_connections \
--point_selects=8 \
--non_index_updates=2 \
--time=$duration \
--report-interval=1 \
run
