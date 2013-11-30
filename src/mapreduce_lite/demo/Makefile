# This file is distributed with the Mapreduce Lite package. It gives an example
# of how to compile program using Mapreduce Lite library.

include Makefile.rules

PARALGO_LIB_DIR = ../lib/paralgo
EXTERNAL_LIBS =                         \
 $(PARALGO_LIB_DIR)/libmapreduce_lite.a \
 $(PARALGO_LIB_DIR)/libsorted_buffer.a  \
 $(PARALGO_LIB_DIR)/libstrutil.a        \
 $(PARALGO_LIB_DIR)/libhash.a           \
 $(PARALGO_LIB_DIR)/libbase.a           \
 $(PARALGO_LIB_DIR)/libsystem.a

BUILD_TARGETS = mrl-wordcount

all : $(BUILD_TARGETS)

mrl-wordcount : wordcount.cc
	$(CXX) $(CXXFLAGS) -o $@ $+ $(EXTERNAL_LIBS) $(LDFLAGS)

clean :
	rm -rf $(BUILD_TARGETS) *.o *.a *.exe *~ *.stackdump *.dSYM

