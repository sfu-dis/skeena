#!/bin/bash
set -x
set -e

tables=$1
table_size=$2
n_connections=$3
p_innodb=$4
duration=$5

if [ "$#" -ne 5 ]; then
  echo "Usage: ./cross_write_only.sh <tables> <table_size> <connections> <innodb%> <duration>" >&2
  exit 1
fi

if [ $p_innodb -lt 0 ] || [ $p_innodb -gt 100 ]; then
    echo "innodb% should have a value between 0 and 100." >&2
      exit 1
fi

# Write only workload
taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79 sysbench ./sysbench/oltp_write_only_cross.lua \
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
--non_index_updates=10 \
--time=$duration \
--report-interval=1 \
run
