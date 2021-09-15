#!/bin/bash
rm -rf /dev/shm/tzwang/mysql/* && \
rm /dev/shm/tzwang/ermia/* -rf && \
./bin/mysqld --initialize && \
rm /dev/shm/tzwang/ermia/* -rf && \
LD_PRELOAD=/usr/lib/libjemalloc.so \
taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94 ./bin/mysqld
