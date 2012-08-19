/**
 * @file tsqr.cc
 * Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
 * @author David F. Gleich
 * @author Austin R. Benson
 */

/**
 * History
 * -------
 * :2011-01-28: Initial coding
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string>

#include <algorithm>
#include <vector>

#include "typedbytes.h"
#include "sparfun_util.h"
#include "tsqr_util.h"

class MatrixHandler {
  MatrixHandler(TypedBytesInFile& in, TypedBytesOutFile& out,
                size_t blocksize, size_t rows_per_record)
    : in_(in), out_(out),
      blocksize_(blocksize), rows_per_record_(rows_per_record),
      num_cols_(0), num_rows_(0), num_total_rows_(0) {}

  virtual void mapper() {
    typedbytes_opaque key;
    std::vector<double> row;
    first_row();
    while (!feof(in_.stream)) {
      if (read_key_val_pair(in_, key, row) == false) {
	if (feof(in_.stream)) {
	  break;
	} else {
	  hadoop_message("invalid key: row %i\n", num_total_rows_);
	  exit(-1);
	}
      }
      collect(key, row);
    }
    close();
    hadoop_status("final output");
    output();
  }
    
  /** Allocate the local matrix and set to zero
   * @param nr the number of rows
   * @param nc the number of cols
   */
  virtual void alloc(size_t nr, size_t nc) {
    local_matrix_.resize(nr * nc);
    for (size_t i = 0; i < nr * nc; ++i) {
      local_matrix_[i] = 0.;
    }
    num_rows_ = nr;
    num_cols_ = nc;
    num_local_rows_ = 0;
  }    
    
  /** Handle the first input row.
   * The first row of the input is special, and so we handle
   * it differently.
   */
  virtual void first_row() {
    typedbytes_opaque key;
    std::vector<double> row;
    read_key_val_pair(in_, key, row);
    // TODO(arbenson) check for error here
    num_cols_ = row.size();
    hadoop_message("matrix size: %zi num_cols_, up to %i localrows\n", 
		   num_cols_, blocksize_ * num_cols_);
    alloc(blocksize_ * num_cols_, num_cols_); 
    add_row(row);
  }
    
  // read in a row and add it to the local matrix
  virtual void add_row(const std::vector<double>& row) {
    assert(row.size() == num_cols_);
    assert(num_local_rows_ < num_rows_);
    // store by column
    for (size_t k = 0; k < rows_per_record_; ++k) {
      size_t i = 0;
      for (size_t j = 0; j < num_cols_; ++j) {
	local_matrix_[num_local_rows_ + j * num_rows_] = row[i];
	++i;
      }
      // increment the number of local rows
      ++num_local_rows_;
      ++num_total_rows_;
    }
  }
  
  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {
    add_row(value);
    if (num_local_rows_ >= num_rows_) {
      hadoop_message("about to compress\n");
      compress();
      hadoop_message("compress appears successful\n");
      hadoop_counter("compress", 1);
    }
  }

  virtual void close() {
    compress();
  }

 public:
  TypedBytesInFile& in_;
  TypedBytesOutFile& out_;

  size_t blocksize_;
  size_t rows_per_record_;
  size_t num_cols_;
  size_t num_rows_;        // the maximum number of rows of the local matrix
  size_t num_local_rows_;  // the current number of local rows
  size_t num_total_rows_;  // the total number of rows processed
    
  std::vector<double> local_matrix_;
};

void raw_mapper(TypedBytesInFile& in, TypedBytesOutFile& out, size_t blocksize,
                size_t rows_per_record, bool use_chol) {
  if (!use_chol) {
    SerialTSQR map(in, out, blocksize, rows_per_record);
    map.mapper();
  } else {
    AtA map(in, out, blocksize, rows_per_record);
    map.mapper();    
  }
}

void raw_reducer(TypedBytesInFile& in, TypedBytesOutFile& out, size_t blocksize,
		 size_t rows_per_record, bool use_chol, int iteration) {
  if (!use_chol) {
    raw_mapper(in, out, blocksize, rows_per_record, use_chol);
  } else {
    if (iteration == 0) {
      RowSum red(in, out, blocksize, rows_per_record);
      red.reducer(); 
    } else {
      Cholesky red(in, out, blocksize, rows_per_record);
      red.reducer(); 
    }
    return;    
  }
}

void usage() {
  fprintf(stderr, "usage: tsqr map|reduce [blocksize] [rowsperrecord] [cholesky]\n");
  exit(-1);
}

// TODO(arbenson): real command-line options
int main(int argc, char** argv) {  
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);
    
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);
    
  if (argc < 2) {
    usage();
  }
    
  size_t blocksize = 3;
  if (argc > 2) {
    int bs = atoi(argv[2]);
    if (bs <= 0) {
      fprintf(stderr, 
              "Error: blocksize \'%s\' is not a positive integer\n",
              argv[2]);
    }
    blocksize = (size_t) bs;
  }

  size_t rows_per_record = 1;
  if (argc > 3) {
    int rpr = atoi(argv[3]);
    if (rpr <= 0) {
      fprintf(stderr, "Error: \'%s\' rows per record is not a positive integer\n", argv[3]);
    }
    rows_per_record = (size_t) rpr;
  }

  bool use_chol = false;
  if (argc > 4) {
    char *chol = argv[4];
    if (!strcmp(chol, "chol") || !strcmp(chol, "cholesky")) {
      use_chol = true;
    }
  }

  int it = 0;
  if (argc > 5) {
    it = atoi(argv[5]);
  }

  char* operation = argv[1];
  if (strcmp(operation,"map") == 0) {
    raw_mapper(in, out, blocksize, rows_per_record, false);
  } else if (strcmp(operation,"reduce") == 0) {
    raw_reducer(in, out, blocksize, rows_per_record, false, it);
  } else {
    usage();
  }
  return (0);
}
