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

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../..
subdir       = contrib/ermia_fdw
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
# install: install-data

# install-data: installdirs
# 	$(INSTALL_DATA) $(addprefix $(srcdir)/, $(DATA)) '$(DESTDIR)$(datadir)/extension/'

# installdirs:
# 	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'

# uninstall: uninstall-data

# uninstall-data:
# 	rm -f $(addprefix '$(DESTDIR)$(datadir)/extension'/, $(notdir $(DATA)))

# .PHONY: install-data install-headers uninstall-data uninstall-headers

# clean:
# 	rm -rf $(OBJS)
