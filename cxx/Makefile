# Makefile designed to run on the icme-hadoop1 cluster at
# ICME, Stanford University
CXX=g++
ATLAS=./atlas
CC=$(CXX)
CXXFLAGS=-Wall -O3  -std=c++0x
LDFLAGS=-L$(ATLAS) -llapack -lf77blas -lcblas -latlas 

# note: NERSC machines need mkl and gcc modules
NERSC := $(shell uname -r)
ifneq (, $(filter %nersc,$(NERSC)))
  CXX=g++
  LDFLAGS=$(MKL) -lpthread
endif

SRC=tsqr.cc direct_tsqr.cc sparfun_util.cc typedbytes.cc tsqr_util.cc MatrixHandler.cc
TSQR_OBJS=tsqr.o direct_tsqr.o sparfun_util.o typedbytes.o tsqr_util.o MatrixHandler.o

all: tsqr

tsqr: $(TSQR_OBJS) $(SRC)
	$(CC) $(CXXFLAS) $(LDFLAGS) -o tsqr $(TSQR_OBJS)

tsqr.o: tsqr.cc sparfun_util.cc typedbytes.cc MatrixHandler.cc direct_tsqr.cc
	$(CC) $(CXXFLAGS) $(LDFLAGS) -c tsqr.cc -o tsqr.o

sparfun_util.o: sparfun_util.cc
	$(CC) $(CXXFLAGS) -c sparfun_util.cc -o sparfun_util.o

direct_tsqr.o: direct_tsqr.cc typedbytes.cc typedbytes.h tsqr_util.h
	$(CC) $(CXXFLAGS) $(LDFLAGS) -c direct_tsqr.cc -o direct_tsqr.o

typedbytes.o: typedbytes.cc typedbytes.h
	$(CC) $(CXXFLAGS) $(LDFLAGS) -c typedbytes.cc -o typedbytes.o

MatrixHandler.o: MatrixHandler.cc
	$(CC) $(CXXFLAGS) $(LDFLAGS) -c MatrixHandler.cc -o MatrixHandler.o

clean:
	rm -rf *.o dump_typedbytes_info tsqr
