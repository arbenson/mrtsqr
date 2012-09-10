// Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
// David F. Gleich
// Austin R. Benson

#include <stdio.h>
#include <stdlib.h>

#include "sparfun_util.h"
#include "tsqr_util.h"
#include "mrmc.h"

// TODO(arbenson): real command-line options

void handle_direct_tsqr(int argc, char **argv) {
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  if (argc < 1) {
    fprintf(stderr, "ERROR: missing stage!\n");
  }
  size_t stage = atoi(argv[0]);

  if (stage != 1 && argc < 2) {
    fprintf(stderr, "ERROR: missing ncols!\n");
  }

  if (stage == 1) {
    FullTSQRMap1 map(in, out, 5, 1);
    map.mapper();
  } else if (stage == 2) {
    size_t ncols = atoi(argv[1]);
    FullTSQRReduce2 map(in, out, 5, 1, ncols);
    map.mapper();
  } else if (stage == 3) {
    size_t ncols = atoi(argv[1]);
    FullTSQRMap3 map(in, out, 5, 1, ncols);
    map.mapper();
  }
}

void handle_indirect_tsqr(int argc, char **argv) {

}

void handle_cholesky_tsqr(int argc, char **argv) {

}

int main(int argc, char **argv) {  
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);

  if (argc < 2) {
    fprintf(stderr, "ERROR: unknown TSQR type\n");
    return -1;
  }

  if (!strcmp(argv[1], "direct")) {
    handle_direct_tsqr(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "indirect")) {
    handle_indirect_tsqr(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "cholesky")) {
    handle_cholesky_tsqr(argc - 2, argv + 2);
  }

  return 0;
}
