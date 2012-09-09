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

BASE=MatrixHandler sparfun_util typedbytes tsqr_util
BASE_SRC=$(addsuffix .cc, $(BASE))

TSQR_ALL=main direct_tsqr SerialTSQR CholeskyQR $(BASE)

OBJ_OUT=tsqr-objs

SRC=$(addsuffix .cc, $(TSQR_ALL))
OBJS=$(addsuffix .o, $(TSQR_ALL))

all: tsqr

tsqr: $(OBJS) $(SRC)
	$(CC) $(CXXFLAS) $(LDFLAGS) -o tsqr $(OBJS)

main.o: $(SRC)
SerialTSQR.o: SerialTSQR.cc $(BASE_SRC)
CholeskyQR.o: CholeskyQR.cc $(BASE_SRC)
direct_tsqr.o: direct_tsqr.cc $(BASE_SRC)
MatrixHandler.o: $(BASE_SRC)

clean:
	rm -rf *.o dump_typedbytes_info tsqr
