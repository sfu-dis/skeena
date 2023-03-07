# contrib/ermia_fdw/Makefile
PGFILEDESC   = "ermia_fdw - foreign data wrapper for ERMIA"
MODULE_big   = ermia_fdw
EXTENSION    = ermia_fdw
REGRESS      = ermia_fdw
DATA 				 = ermia_fdw--1.0.sql
EXTRA_CLEAN  = sql/ermia_fdw.sql expected/ermia_fdw.out
#PG_LDFLAGS  += -L/home/khuang/projects/skeena-new/skeena/ermia_fdw/build/lib
#SHLIB_LINK   = -lermia_api
OBJS         = ermia_fdw.o

top_builddir = ../../../..
subdir       = src/backend/storage/ermia

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/backend/common.mk