#-------------------------------------------------------------------------
#
# Makefile for src/bin/memstore
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/memstore/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "memstore - in-memory storage for SMGR"
PGAPPICON=win32

subdir = src/bin/memstore
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LDFLAGS_INTERNAL += -lpthread

all: memstore

memstore: memstore.cpp
	g++ $(CPPFLAGS) -pthread memstore.cpp $(LDFLAGS)  $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) memstore$(X) '$(DESTDIR)$(bindir)/memstore$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/memstore$(X)'

clean distclean maintainer-clean:
	rm -f memstore$(X) memstore.o 
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
