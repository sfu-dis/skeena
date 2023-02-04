# contrib/ermia_fdw/Makefile

MODULE_big = ermia_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	ermia_fdw.o
PGFILEDESC = "ermia_fdw - foreign data wrapper for ERMIA"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = ermia_fdw
DATA = ermia_fdw--1.0.sql

REGRESS = ermia_fdw

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/ermia_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
