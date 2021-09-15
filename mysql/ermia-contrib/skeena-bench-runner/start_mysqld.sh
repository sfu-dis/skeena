MYSQLD_BIN_DIR="/home/void001/Projects/mysql-ermia/build/bin"
ERMIA_LOG_DIR="/dev/shm/void001/ermia-log" 
rm -rfv $ERMIA_LOG_DIR
rm -rfv /dev/shm/void001/mysql/data
mkdir $ERMIA_LOG_DIR
mkdir -p /dev/shm/void001/mysql/tmp
mkdir -p /dev/shm/void001/mysql/data
$MYSQLD_BIN_DIR/mysqld --initialize --skip-grant-tables --disable-log-bin --ermia-ermia-log-dir="$ERMIA_LOG_DIR" --datadir="/dev/shm/void001/mysql/data" $@
rm -rf $ERMIA_LOG_DIR/
mkdir -p $ERMIA_LOG_DIR
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78 $MYSQLD_BIN_DIR/mysqld --skip-grant-tables --disable-log-bin --ermia-ermia-log-dir="$ERMIA_LOG_DIR" --datadir="/dev/shm/void001/mysql/data" $@
