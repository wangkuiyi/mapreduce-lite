CXX=g++ -Wall -Wno-sign-compare -Werror -O2
AR=ar rcs


BUILD_TARGETS = libstrutil.a split_string_test strcodec_test stringprintf_test
DIST_DIR = ../../distribution/strutil


all : $(BUILD_TARGETS)

libstrutil.a : split_string.o strcodec.o stringprintf.o
	$(AR) libstrutil.a $+

split_string_test : split_string_test.o libstrutil.a ../base/libbase.a
	$(CXX) -o split_string_test split_string_test.o -L. -lstrutil -L../base -lbase

strcodec_test : strcodec_test.o libstrutil.a ../base/libbase.a
	$(CXX) -o strcodec_test strcodec_test.o -L. -lstrutil -L../base -lbase

stringprintf_test : stringprintf_test.o ../base/libbase.a
	$(CXX) -o stringprintf_test stringprintf_test.o -L. -lstrutil -L../base -lbase

dist : $(BUILD_TARGETS)
	rm -rf $(DIST_DIR)
	mkdir -p $(DIST_DIR)
	cp $(BUILD_TARGETS) $(DIST_DIR)
	cp *.hh $(DIST_DIR)

clean :
	rm -rf $(BUILD_TARGETS) *.o *.a *.exe *~ *.stackdump *.dSYM
