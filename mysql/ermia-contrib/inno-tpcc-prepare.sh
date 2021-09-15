set -e
cat > prepare.SQL <<EOF
DROP DATABASE IF EXISTS dbtest;
CREATE DATABASE dbtest;
EOF

mysql < prepare.SQL
./inno-tpcc.lua --mysql-socket=/dev/shm/void001/mysql/mysqld.sock --mysql-db=dbtest --scale=$1 --threads=$2 --engine_mapping="$3" --mysql-storage-engine=ERMIA --use-fk=0 --report-interval=1 --random-warehouse=false --transaction-type=20 --time=010 --mysql-ignore-errors=1030 --debug prepare
