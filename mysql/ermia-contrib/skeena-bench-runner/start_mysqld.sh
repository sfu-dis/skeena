MYSQLD_BIN_DIR="/home/$USER/Projects/mysql-ermia/build/bin"
ERMIA_LOG_DIR="/dev/shm/$USER/ermia-log"
rm -rfv $ERMIA_LOG_DIR
rm -rfv /dev/shm/$USER/mysql/data
mkdir $ERMIA_LOG_DIR
mkdir -p /dev/shm/$USER/mysql/tmp
mkdir -p /dev/shm/$USER/mysql/data
$MYSQLD_BIN_DIR/mysqld --initialize --skip-grant-tables --disable-log-bin --ermia-ermia-log-dir="$ERMIA_LOG_DIR" --datadir="/dev/shm/$USER/mysql/data" $@
rm -rf $ERMIA_LOG_DIR/
mkdir -p $ERMIA_LOG_DIR
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so taskset -c <server threads> $MYSQLD_BIN_DIR/mysqld --skip-grant-tables --disable-log-bin --ermia-ermia-log-dir="$ERMIA_LOG_DIR" --datadir="/dev/shm/$USER/mysql/data" $@
