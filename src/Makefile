# CS165 Makefile (C++ Version)

# Target executables
all: client server

# C++ compiler settings
CXX = g++
CXXFLAGS = -std=c++17 -g -ggdb3 -Wall -Wextra -pedantic -pthread -O$(O) -I$(INCLUDES)

# Default optimization level
O ?= 0

# Linker flags and libraries
LDFLAGS =
LIBS =

# Include directory
INCLUDES = include

# Unix socket path (escaped for makefile format)
SOCK_PATH = \"/tmp/cs165_unix_socket\"

# --- Automatic Dependency Tracking ---
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif

# Dependency compilation flags (using CXXFLAGS)
DEPCFLAGS = -MMD -MP -MF $(DEPSDIR)/$*.d -DSOCK_PATH=$(SOCK_PATH)

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

# --- Compilation Rules ---

# Object files (compile .cpp files)
%.o : %.cpp $(BUILDSTAMP)
	$(CXX) $(CXXFLAGS) $(DEPCFLAGS) -o $@ -c $<

%.o : %.c $(BUILDSTAMP)
	$(CXX) $(CXXFLAGS) $(DEPCFLAGS) -o $@ -c $<

# --- Linking ---

# Client executable
client: client.o utils.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

# Server executable
server: server.o parse.o utils.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

# --- Clean Targets ---

clean:
	rm -f client server *.o *~ *.bak core *.core $(DEPSDIR)/* $(SOCK_PATH)

distclean: clean
	rm -rf $(DEPSDIR)

.PHONY: all clean distclean