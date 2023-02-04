#!/bin/bash
set -x
set -e

tables=$1
table_size=$2
n_connections=$3
p_pgsql=$4
duration=$5

if [ "$#" -ne 5 ]; then
  echo "Usage: ./pgsql_read_only.sh <tables> <table_size> <connections> <pgsql%> <duration>" >&2
  exit 1
fi

if [ $p_pgsql -lt 0 ] || [ $p_pgsql -gt 100 ]; then
  echo "pgsql% should have a value between 0 and 100." >&2
  exit 1
fi

# Read only workload
taskset -c 1-79:2 sysbench ./sysbench/oltp_read_only_pgsql.lua \
--pgsql_percentage=$p_pgsql \
--create_secondary=off \
--rand-type=uniform \
--tables=$tables \
--range_selects=off \
--table_size=$table_size \
--pgsql-db=dbtest \
--db-driver=pgsql \
--pgsql-host=/tmp \
--pgsql-port=5432 \
--pgsql-user=$USER \
--threads=$n_connections \
--point_selects=10 \
--time=$duration \
--report-interval=1 \
run
