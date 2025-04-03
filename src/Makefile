# CS165 Makefile (C++ Version)

# Target executables
all: client server lsm_tests benchmark bloom_tests

# C++ compiler settings
CXX = g++

# high performance flags
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pedantic -pthread -I$(INCLUDES)

# Linker flags and libraries
LDFLAGS =
LIBS =

# Include directory
INCLUDES = include

# Unix socket path (escaped for makefile format)
SOCK_PATH = \"/tmp/my_unix_socket\"

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
server: server.o parse.o utils.o lsm_tree.o bloom_filter.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

benchmark: benchmark.o lsm_tree.o bloom_filter.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

lsm_tests: test_main.o lsm_tree.o bloom_filter.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

bloom_tests: bloom_filter.o test_bloom_filter.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)


# --- Clean Targets ---

clean:
	rm -rf client server benchmark *.o *~ *.bak core *.core $(DEPSDIR)/* $(SOCK_PATH)

distclean: clean
	rm -rf $(DEPSDIR)

.PHONY: all clean distclean