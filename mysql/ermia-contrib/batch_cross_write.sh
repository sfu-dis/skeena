#!/bin/bash
set -e
set -x

connections=( 96 )
percentage=( 100 )
for i in "${connections[@]}"
do
  for j in "${percentage[@]}"
  do
    taskset -c <client threads> sysbench ./sysbench/oltp_write_only_cross.lua --non_index_updates=10 --mysql-ignore-errors=1180,1030,1412 --innodb_percentage=$j --mysql_storage_engine=ERMIA --create_secondary=off --rand-type=uniform --tables=250 --range_selects=off --table_size=250000 --mysql-db=dbtest --db-driver=mysql --mysql-socket=/dev/shm/$USER/mysql/mysqld.sock --threads=$i --time=60 --report-interval=1 run #> ./sysbench/sysbench-log/cross_write_only_${i}_innodb${j}.log
  done
done
