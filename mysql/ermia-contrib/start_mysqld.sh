#!/bin/bash -
#===============================================================================
#
#          FILE: start_mysqld.sh
#
#         USAGE: ./start_mysqld.sh
#
#   DESCRIPTION: 
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 07/12/2021 02:21:36 AM
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so taskset -c 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74,76,78 /home/void001/skeena/build/bin/mysqld $@
