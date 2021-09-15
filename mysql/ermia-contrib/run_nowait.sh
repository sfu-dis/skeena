#!/bin/bash -
#===============================================================================
#
#          FILE: run.sh
#
#         USAGE: ./run.sh
#
#   DESCRIPTION: 
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 07/11/2021 09:40:02 PM
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error

if [ "$#" -ne 3 ]; then
    echo "run.sh <scale> <txn_type> <mapping>"
    exit 1
fi
scale=$1
txn=$2
mapping=$3
taskset -c 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79 ./tpcc.lua --mysql-socket=/dev/shm/void001/mysql/mysqld.sock --mysql-db=dbtest --scale=$scale --threads=$scale --mysql-storage-engine=ERMIA --use-fk=0 --report-interval=1 --random-warehouse=true --transaction-type=$txn --time=60 --mysql-ignore-errors=1180,1030,1213 --rand-type=uniform run | tee logs/tpcc-type-$txn-scale-$scale-map-$mapping.log
