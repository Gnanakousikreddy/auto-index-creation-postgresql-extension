# contrib/auto_index/Makefile

MODULE_big = auto_index
OBJS = \
	$(WIN32RES) \
	auto_index.o \
	auto_index_worker.o

SHLIB_LINK += -lm

NO_BITCODE = 1

EXTENSION = auto_index
DATA = auto_index--1.0.sql
PGFILEDESC = "auto_index - Autonomous Index Creation"

USE_PGXS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/auto_index
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif