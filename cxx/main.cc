// Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
// David F. Gleich
// Austin R. Benson

#include <stdio.h>
#include <stdlib.h>

#include "sparfun_util.h"
#include "tsqr_util.h"
#include "mrmc.h"

// TODO(arbenson): real command-line options
int main(int argc, char** argv) {  
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);

  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  int stage = 1;
  if (argc > 2) {
    stage = atoi(argv[2]);
  }
  size_t ncols = 10;
  if (argc > 3) {
    ncols = atoi(argv[3]);
  }
  if (stage == 1) {
    FullTSQRMap1 map(in, out, 5, 1);
    map.mapper();
  } else if (stage == 2) {
    FullTSQRReduce2 map(in, out, 5, 1, ncols);
    map.mapper();
  } else if (stage == 3) {
    FullTSQRMap3 map(in, out, 5, 1, ncols);
    map.mapper();
  }
  return 0;
}
