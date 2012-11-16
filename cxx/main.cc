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
  fprintf(stderr, "using direct TSQR\n");
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

  // TODO(arbenson): handle rows per record
  if (stage == 1) {
    DirTSQRMap1 map(in, out, 1);
    map.mapper();
  } else if (stage == 2) {
    size_t ncols = atoi(argv[1]);
    DirTSQRReduce2 map(in, out, 1, ncols);
    map.mapper();
  } else if (stage == 3) {
    size_t ncols = atoi(argv[1]);
    DirTSQRMap3 map(in, out, 1, ncols);
    map.mapper();
  }
}

void handle_indirect_tsqr(int argc, char **argv) {
  fprintf(stderr, "using indirect TSQR\n");
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  size_t blocksize = 3;
  if (argc > 0)
    blocksize = atoi(argv[0]);

  size_t rows_per_record = 1;
  if (argc > 1)
    rows_per_record = atoi(argv[1]);

  SerialTSQR map(in, out, blocksize, rows_per_record);
  map.mapper();
}

void handle_cholesky_AtA(int argc, char **argv) {
  fprintf(stderr, "using Cholesky TSQR\n");
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  size_t blocksize = 3;
  if (argc > 0)
    blocksize = atoi(argv[0]);

  size_t rows_per_record = 1;
  if (argc > 1)
    rows_per_record = atoi(argv[1]);

  AtA map(in, out, blocksize, rows_per_record);
  map.mapper();
}

void handle_cholesky_rowsum(int argc, char **argv) {
  fprintf(stderr, "using Cholesky TSQR\n");
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  size_t rows_per_record = 1;
  if (argc > 0)
    rows_per_record = atoi(argv[0]);

  RowSum map(in, out, rows_per_record);
  map.mapper();
}

void handle_cholesky_comp(int argc, char **argv) {
  fprintf(stderr, "using Cholesky TSQR\n");
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  size_t rows_per_record = 1;
  if (argc > 0)
    rows_per_record = atoi(argv[0]);

  Cholesky map(in, out, rows_per_record);
  map.mapper();
}

int main(int argc, char **argv) {  
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);

  if (argc < 2) {
    fprintf(stderr, "ERROR: unknown TSQR type\n");
    return -1;
  }

  // TODO(arbenson): Cholesky QR is broken
  if (!strcmp(argv[1], "direct")) {
    handle_direct_tsqr(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "indirect")) {
    handle_indirect_tsqr(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "ata")) {
    handle_cholesky_AtA(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "rowsum")) {
    handle_cholesky_rowsum(argc - 2, argv + 2);
  } else if (!strcmp(argv[1], "cholesky")) {
    handle_cholesky_comp(argc - 2, argv + 2);
  } else {
    fprintf(stderr, "unknown method!\n");
    return -1;
  }

  return 0;
}
