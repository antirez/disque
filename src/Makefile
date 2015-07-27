# Disque Makefile
# Copyright (C) 2009-2014 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file
#
# The Makefile composes the final FINAL_CFLAGS and FINAL_LDFLAGS using
# what is needed for Disque plus the standard CFLAGS and LDFLAGS passed.
# However when building the dependencies (Jemalloc, Hiredis, ...)
# CFLAGS and LDFLAGS are propagated to the dependencies, so to pass
# flags only to be used when compiling / linking Disque itself DISQUE_CFLAGS
# and DISQUE_LDFLAGS are used instead (this is the case of 'make gcov').
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep', but this is only needed by developers.

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
DEPENDENCY_TARGETS=hiredis linenoise

# Default settings
STD=-std=c99 -pedantic
WARN=-Wall -W
OPT=$(OPTIMIZATION)

PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
else
	MALLOC=libc
endif

# Backwards compatibility for selecting an allocator
ifeq ($(USE_TCMALLOC),yes)
	MALLOC=tcmalloc
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC=tcmalloc_minimal
endif

ifeq ($(USE_JEMALLOC),yes)
	MALLOC=jemalloc
endif

# Override default settings if possible
-include .make-settings

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(DISQUE_CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(DISQUE_LDFLAGS) $(DEBUG)
FINAL_LIBS=-lm
DEBUG=-g -ggdb

ifeq ($(uname_S),SunOS)
	# SunOS
	INSTALL=cp -pf
	FINAL_CFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_LIBS+= -ldl -lnsl -lsocket -lresolv -lpthread
else
ifeq ($(uname_S),Darwin)
	# Darwin (nothing to do)
else
ifeq ($(uname_S),AIX)
        # AIX
        FINAL_LDFLAGS+= -Wl,-bexpall
        FINAL_LIBS+= -pthread -lcrypt -lbsd

else
	# All the other OSes (notably Linux)
	FINAL_LDFLAGS+= -rdynamic
	FINAL_LIBS+= -pthread
endif
endif
endif
# Include paths to dependencies
FINAL_CFLAGS+= -I../deps/hiredis -I../deps/linenoise

ifeq ($(MALLOC),tcmalloc)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc
endif

ifeq ($(MALLOC),tcmalloc_minimal)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc_minimal
endif

ifeq ($(MALLOC),jemalloc)
	DEPENDENCY_TARGETS+= jemalloc
	FINAL_CFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_LIBS+= ../deps/jemalloc/lib/libjemalloc.a -ldl
endif

DISQUE_CC=$(QUIET_CC)$(CC) $(FINAL_CFLAGS)
DISQUE_LD=$(QUIET_LINK)$(CC) $(FINAL_LDFLAGS)
DISQUE_INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif

DISQUE_SERVER_NAME=disque-server
DISQUE_SERVER_OBJ=adlist.o ae.o anet.o dict.o server.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o sha1.o release.o networking.o util.o object.o config.o aof.o debug.o syncio.o cluster.o endianconv.o slowlog.o bio.o memtest.o crc64.o setproctitle.o blocked.o latency.o sparkline.o rio.o job.o queue.o skiplist.o ack.o
DISQUE_CLI_NAME=disque
DISQUE_CLI_OBJ=anet.o adlist.o disque-cli.o zmalloc.o release.o anet.o ae.o crc64.o
DISQUE_CHECK_AOF_NAME=disque-check-aof
DISQUE_CHECK_AOF_OBJ=disque-check-aof.o

all: $(DISQUE_SERVER_NAME) $(DISQUE_CLI_NAME) $(DISQUE_CHECK_AOF_NAME)
	@echo ""
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

.PHONY: all

# Deps (use make dep to generate this)
include Makefile.dep

dep:
	$(DISQUE_CC) -MM *.c > Makefile.dep

.PHONY: dep

persist-settings: distclean
	echo STD=$(STD) >> .make-settings
	echo WARN=$(WARN) >> .make-settings
	echo OPT=$(OPT) >> .make-settings
	echo MALLOC=$(MALLOC) >> .make-settings
	echo CFLAGS=$(CFLAGS) >> .make-settings
	echo LDFLAGS=$(LDFLAGS) >> .make-settings
	echo DISQUE_CFLAGS=$(DISQUE_CFLAGS) >> .make-settings
	echo DISQUE_LDFLAGS=$(DISQUE_LDFLAGS) >> .make-settings
	echo PREV_FINAL_CFLAGS=$(FINAL_CFLAGS) >> .make-settings
	echo PREV_FINAL_LDFLAGS=$(FINAL_LDFLAGS) >> .make-settings
	-(cd ../deps && $(MAKE) $(DEPENDENCY_TARGETS))

.PHONY: persist-settings

# Prerequisites target
.make-prerequisites:
	@touch $@

# Clean everything, persist settings and build dependencies if anything changed
ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings
endif

ifneq ($(strip $(PREV_FINAL_LDFLAGS)), $(strip $(FINAL_LDFLAGS)))
.make-prerequisites: persist-settings
endif

# disque-server
$(DISQUE_SERVER_NAME): $(DISQUE_SERVER_OBJ)
	$(DISQUE_LD) -o $@ $^ $(FINAL_LIBS)

# disque-cli
$(DISQUE_CLI_NAME): $(DISQUE_CLI_OBJ)
	$(DISQUE_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/linenoise/linenoise.o $(FINAL_LIBS)

# disque-check-aof
$(DISQUE_CHECK_AOF_NAME): $(DISQUE_CHECK_AOF_OBJ)
	$(DISQUE_LD) -o $@ $^ $(FINAL_LIBS)

# Because the jemalloc.h header is generated as a part of the jemalloc build,
# building it should complete before building any other object. Instead of
# depending on a single artifact, build all dependencies first.
%.o: %.c .make-prerequisites
	$(DISQUE_CC) -c $<

clean:
	rm -rf $(DISQUE_SERVER_NAME) $(DISQUE_CLI_NAME) $(DISQUE_CHECK_AOF_NAME) *.o *.gcda *.gcno *.gcov disque.info lcov-html

.PHONY: clean

distclean: clean
	-(cd ../deps && $(MAKE) distclean)
	-(rm -f .make-*)

.PHONY: distclean

test: $(DISQUE_SERVER_NAME) $(DISQUE_CHECK_AOF_NAME)
	@(cd ..; ./runtest)

check: test

lcov:
	$(MAKE) gcov
	@(set -e; cd ..; ./runtest --clients 1)
	@geninfo -o disque.info .
	@genhtml --legend -o lcov-html disque.info

.PHONY: lcov

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32"

gcov:
	$(MAKE) DISQUE_CFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" DISQUE_LDFLAGS="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION="-O0"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

src/help.h:
	@../utils/generate-command-help.rb > help.h

install: all
	@mkdir -p $(INSTALL_BIN)
	$(DISQUE_INSTALL) $(DISQUE_SERVER_NAME) $(INSTALL_BIN)
	$(DISQUE_INSTALL) $(DISQUE_CLI_NAME) $(INSTALL_BIN)
	$(DISQUE_INSTALL) $(DISQUE_CHECK_AOF_NAME) $(INSTALL_BIN)
