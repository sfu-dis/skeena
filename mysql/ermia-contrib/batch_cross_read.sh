#!/bin/bash
set -e
set -x

connections=( 1 48 96 144 )
percentage=( 0 25 50 75 100 )
for i in "${connections[@]}"
do
  for j in "${percentage[@]}"
  do
    # Ignore error 1180, 1030, 1412 of cross-commit failure/SI conflict/MISSING_HISTORY (read view too old with data already purged)
    taskset -c 48-95 sysbench ./sysbench/oltp_read_only_cross.lua --mysql-ignore-errors=1180,1030,1412 --innodb_percentage=$j --mysql_storage_engine=ERMIA --create_secondary=off --rand-type=uniform --tables=250 --range_selects=off --table_size=250000 --mysql-db=dbtest --db-driver=mysql --mysql-socket=/dev/shm/$USER/mysql/mysqld.sock --threads=$i --time=30 --report-interval=1 run > ./sysbench/sysbench-log/cross_read_only_${i}_innodb${j}.log
  done
done
