EXTENSION    = dummy_data
EXTVERSION   = 2.0
MODULE_big   = dummy_data
OBJS         =  dummy_data.o 
DOCS         = $(wildcard *.md)

BUILD_DIR = $(shell pwd)

LOADLOCAL=dummy_data--1.0.sql

all: $(LOADLOCAL) $(EXTENSION)--$(EXTVERSION).sql

install: 

clobber:: clean 
	-rm dummy_data--1.0.sql

# backwards compatibility: loading the library from the build directory
$(LOADLOCAL) : $(LOADLOCAL).in
	cat $< | sed 's@BUILD_DIR@$(BUILD_DIR)@' > $@

$(EXTENSION)--$(EXTVERSION).sql : $(EXTENSION).sql
	@cp $< $@

DATA_built = $(EXTENSION)--$(EXTVERSION).sql
# DATA= $(EXTENSION).sql
PG_CONFIG ?= pg_config
REGRESS      = dummy_data_test dummy_data_test2

PG_TEST_VERSION ?= $(MAJORVERSION)
REGRESS_OPTS = --inputdir=sql --load-language=plpgsql --dbname=regression

subdir = contrib/ermia_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
