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
#include <time.h>

#include <string>
#include <algorithm>
#include <vector>
#include <list>

#include "typedbytes.h"
#include "sparfun_util.h"
#include "tsqr_util.h"

std::string pseudo_uuid() {
  char buf[32];
  snprintf(buf, sizeof(buf), "%x%x%x%x",
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000));
  std::string uuid(buf);
  return uuid;
}

class MatrixHandler {
public:
  MatrixHandler(TypedBytesInFile& in, TypedBytesOutFile& out,
                size_t blocksize, size_t rows_per_record)
    : in_(in), out_(out),
      blocksize_(blocksize), rows_per_record_(rows_per_record),
      num_cols_(0), num_rows_(0), num_total_rows_(0) {}

  ~MatrixHandler() {}

  void read_full_row(std::vector<double>& row) {
    row.clear();
    TypedBytesType code = in_.next_type();
    if (code == TypedBytesVector) {
      typedbytes_length len = in_.read_typedbytes_sequence_length();
      row.reserve((size_t)len);
      for (size_t i = 0; i < (size_t)len; ++i) {
        TypedBytesType nexttype = in_.next_type();
        if (in_.can_be_double(nexttype)) {
          row.push_back(in_.convert_double());
        } else {
          fprintf(stderr, 
                  "error: row %zi, col %zi has a non-double-convertable type\n",
                  num_total_rows_, row.size());
          exit(-1);
        }
      }
    } else if (code == TypedBytesList) {
      TypedBytesType nexttype = in_.next_type();
      while (nexttype != TypedBytesListEnd) {
        if (in_.can_be_double(nexttype)) {
          row.push_back(in_.convert_double());
        } else {
          hadoop_message("row has a non-double-convertable type!!\n");
          fprintf(stderr, 
                  "error: row %zi, col %zi has a non-double-convertable type\n",
                  num_total_rows_, row.size());
          exit(-1);
        }
        nexttype = in_.next_type();
      }
    } else if (code == TypedBytesString) {
      typedbytes_length len = in_.read_string_length();
      row.resize(len / 8);
      in_.read_string_data((unsigned char *)&row[0], (size_t)len);
    } else {
      hadoop_message("row is not a list, vector, or string\n");
      hadoop_message("code is: %d\n", code);
      fprintf(stderr,
              "error: row %zi is a not a list, vector, or string\n", num_total_rows_);
      exit(-1);
    }
  }

  bool read_key_val_pair(typedbytes_opaque& key,
                         std::vector<double>& value) {
    if (!in_.read_opaque(key)) {
      return false;
    }
    read_full_row(value); 
    return true;
  }

  virtual void mapper() {
    typedbytes_opaque key;
    std::vector<double> row;
    first_row();
    while (!feof(in_.get_stream())) {
      if (!read_key_val_pair(key, row)) {
        if (feof(in_.get_stream())) {
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
    read_key_val_pair(key, row);
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
  
  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) = 0;

  virtual void output() = 0;

  virtual void close() = 0;

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

class SerialTSQR : public MatrixHandler {
public:
  SerialTSQR(TypedBytesInFile& in, TypedBytesOutFile& out,
            size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {}
  virtual ~SerialTSQR() {}

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {
    add_row(value);
    if (num_local_rows_ >= num_rows_) {
      hadoop_message("about to compress\n");
      compress();
      hadoop_message("compress appears successful\n");
      hadoop_counter("compress", 1);
    }
  }

  void close() {
    compress();
  }

  // compress the local QR factorization
  void compress() {
    // compute a QR factorization
    double t0 = sf_time();
    if (lapack_qr(&local_matrix_[0], num_rows_, num_cols_, num_local_rows_)) {
      double dt = sf_time() - t0;
      hadoop_counter("lapack time (millisecs)", (int) (dt * 1000.));
    } else {
      hadoop_message("lapack error\n");
      exit(-1);
    }
    if (num_cols_ < num_local_rows_) {
      num_local_rows_ = num_cols_;
    }
  }

  // Output the matrix with random keys for the rows.
  void output() {
    for (size_t i = 0; i < num_local_rows_; ++i) {
      int rand_int = sf_randint(0, 2000000000);
      out_.write_int(rand_int);
      out_.write_list_start();
      for (size_t j = 0; j < num_cols_; ++j) {
        out_.write_double(local_matrix_[i + j * num_rows_]);
      }
      out_.write_list_end();
    }
  }
};

class Cholesky : public MatrixHandler {
public:
  Cholesky(TypedBytesInFile& in, TypedBytesOutFile& out,
           size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {}

  int read_key() {
    TypedBytesType type = in_.next_type();
    if (type != TypedBytesInteger) {
      hadoop_message("invalid key, TypedBytes code: %d (skipping) \n", type);
      in_.skip_next();
      return -1;
    }
    return in_.read_int();
  }
  
  /** Handle the first input row.
   * The first row of the input is special, and so we handle
   * it differently.
   */
  void first_row() {
    int row_index = read_key();
    std::vector<double> row;
    read_full_row(row);
    num_cols_ = row.size();
    rows_.resize(num_cols_);
    for (int i = 0; i < (int) num_cols_; ++i) {
      rows_[i] = NULL;
    }
    assert(row_index < (int)num_cols_);
    hadoop_message("matrix size: %zi num_cols_, up to %i localrows\n", 
                   num_cols_, blocksize_ * num_cols_);
    add_row(row, row_index);
  }

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {}
    
  // read in a row and add it to the local matrix
  void add_row(const std::vector<double>& row, int row_index) {
    double *new_row = (double *) malloc(num_cols_ * sizeof(double));
    for (int i = 0; i < (int) num_cols_; ++i) {
      new_row[i] = row[i];
    }
    rows_[row_index] = new_row;
  }

  void reducer() {
    mapper();
  }

  void compress() {}
  void close() {}
  
  // Output the row sums
  void output() {
    int dim = (int) num_cols_;
    double *A = (double *) malloc(dim * dim * sizeof(double));
    assert(A);
    // copy matrix and check that all rows were filled
    for (int i = 0; i < dim; ++i) {
      double *curr_row = rows_[i];
      assert(curr_row);
      for (int j = 0; j < dim; ++j) {
        A[i * dim + j] = curr_row[j];
      }
      free(curr_row);
    }

    // call Cholesky once
    double t0 = sf_time();
    lapack_chol(A, dim);
    double dt = sf_time() - t0;
    hadoop_counter("lapack time (millisecs)", (int) (dt * 1000.));

    for (int i = 0; i < dim; ++i) {
      out_.write_int(i);
      out_.write_list_start();
      for (int j = 0; j < dim; ++j) {
        if (j <= i) {
          out_.write_double(A[i * dim + j]);
        } else {
          out_.write_double(0.0);
        }
      }
      out_.write_list_end();
    }
  }

private:
  std::vector<double*> rows_;
};

class AtA : public MatrixHandler {
public:
  AtA(TypedBytesInFile& in, TypedBytesOutFile& out,
      size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {
    local_AtA_ = NULL;
  }

  // Call syrk and store the result in local AtA computation
  void compress() {
    double t0 = sf_time();
    if (!local_AtA_) {
      local_AtA_ = (double *)calloc(num_cols_ * num_cols_, sizeof(double));
      assert(local_AtA_);
    }
    if (lapack_syrk(&local_matrix_[0], local_AtA_, num_rows_, num_cols_,
                    num_local_rows_)) {
      double dt = sf_time() - t0;
      hadoop_counter("lapack time (millisecs)", (int) (dt * 1000.));
    } else {
      hadoop_message("lapack error\n");
      exit(-1);
    }
    num_local_rows_ = 0;
  }
    
  // Output the matrix with key equal to row number
  void output() {
    for (size_t i = 0; i < num_cols_; ++i) {
      out_.write_int(i);
      out_.write_list_start();
      for (size_t j = 0; j < num_cols_; ++j) {
        out_.write_double(local_AtA_[i + j * num_rows_]);
      }
      out_.write_list_end();
    }
  }

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {}
  
  virtual void close() {}

private:
  double *local_AtA_;
};

class RowSum : MatrixHandler {
public:
  RowSum(TypedBytesInFile& in, TypedBytesOutFile& out,
         size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {}

  int read_key() {
    TypedBytesType type = in_.next_type();
    if (type != TypedBytesInteger) {
      hadoop_message("invalid key, TypedBytes code: %d (skipping) \n",type);
      in_.skip_next();
      return -1;
    }
    return in_.read_int();
  }
    
  /** Handle the first input row.
   * The first row of the input is special, and so we handle
   * it differently.
   */
  virtual void first_row() {
    int row_index = read_key();
    std::vector<double> row;
    read_full_row(row);
    num_cols_ = row.size();
    rows_.resize(num_cols_);
    used_.resize(num_cols_);
    assert(row_index < (int) num_cols_);
    for (int i = 0; i < (int) num_cols_; ++i) {
      rows_[i] = NULL;
      used_[i] = false;
    }
    hadoop_message("matrix size: %zi num_cols_, up to %i localrows\n", 
                   num_cols_, blocksize_ * num_cols_);
    add_row(row, row_index);
  }
    
  // read in a row and add it to the local matrix
  virtual void add_row(const std::vector<double>& row, int row_index) {
    assert(row.size() == num_cols_);
    if(used_[row_index]) {
      double *curr_row = rows_[row_index];
      double t0 = sf_time();
      lapack_daxpy((int)num_cols_, curr_row, (double *) &row[0]);
      double dt = sf_time() - t0;
      hadoop_counter("lapack time (millisecs)", (int)(dt * 1000.));
    } else {
      double *new_row = (double *) malloc(num_cols_ * sizeof(double));
      for (int i = 0; i < (int)num_cols_; ++i) {
        new_row[i] = row[i];
      }
      rows_[row_index] = new_row;
      used_[row_index] = true;
    }
  }

  // do-nothing in this case (only need final output)
  void compress() {}

  virtual void close() {}

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {
    int key_converted = 0;
    key_converted = ((int) key[0] << 24) | ((int) key[1] << 16) |
      ((int) key[2] << 8) | ((int) key[3]);
    add_row(value, (int) key_converted);
  }

  virtual void reducer() {
    mapper();
  }  
  
  // Output the row sums
  void output() {
    assert(rows_.size() == used_.size());
    int num_rows_ = (int)rows_.size();
    for (int i = 0; i < num_rows_; ++i) {
      if (used_[i]) {
        out_.write_int(i);
        out_.write_list_start();
        double *curr_row = rows_[i];
        for (size_t j = 0; j < num_cols_; ++j) {
          out_.write_double(curr_row[j]);
        }
        out_.write_list_end();
        free(curr_row);
      }
    }
  }
  
private:
  std::vector<double*> rows_;
  std::vector<bool> used_;
};

class FullTSQRMap1 : public MatrixHandler {
public:
  FullTSQRMap1(TypedBytesInFile& in_, TypedBytesOutFile& out_,
	       size_t blocksize_, size_t rows_per_record_)
    : MatrixHandler(in_, out_, blocksize_, rows_per_record_) {
    mapper_id_ = pseudo_uuid();
  }
  virtual ~FullTSQRMap1() {}

  virtual void first_row() {
    typedbytes_opaque key;
    std::vector<double> row;
    read_key_val_pair(key, row);
    num_cols_ = row.size();
    hadoop_message("matrix size: %zi\n", num_cols_);
    collect(key, row);
  }


  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {
    keys_.push_back(key);
    for (size_t i = 0; i < value.size(); ++i) {
      row_accumulator_.push_back(value[i]);
    }
    ++num_rows_;
  }

  void close() {}

  void output() {
    // Storage for R
    double *R_matrix = (double *) malloc(num_cols_ * num_cols_ * sizeof(double));
    assert(R_matrix);
    size_t num_rows = row_accumulator_.size() / num_cols_;
    hadoop_message("nrows: %d, ncols: %d\n", num_rows, num_cols_);
    lapack_full_qr(&row_accumulator_[0], R_matrix, num_rows, num_cols_, num_rows);

    // output R
    out_.write_list_start();
    // Specify output file
    std::string output_file = "R_" + mapper_id_;
    out_.write_string_stl(output_file);
    // Specify actual key
    out_.write_string_stl(mapper_id_);
    out_.write_list_end();

    hadoop_message("Output: R");
    out_.write_list_start();
    for (size_t i = 0; i < num_cols_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        out_.write_double(R_matrix[i + j * num_cols_]);
      }
    }
    out_.write_list_end();

    // output (Q, keys)
    out_.write_list_start();
    // Specify output file
    output_file = "Q_" + mapper_id_;
    out_.write_string_stl(output_file);
    // Specify actual key
    out_.write_string_stl(mapper_id_);
    out_.write_list_end();

    // start value write
    out_.write_list_start();

    hadoop_message("Output: Q");
    out_.write_list_start();
    for (size_t i = 0; i < row_accumulator_.size(); ++i) {
      out_.write_double(row_accumulator_[i]);
    }
    out_.write_list_end();

    hadoop_message("Output: keys");
    out_.write_list_start();
    for (std::list<typedbytes_opaque>::iterator it = keys_.begin();
         it != keys_.end(); ++it) {
      typedbytes_opaque key = *it;
      out_.write_opaque_type(&key[0], key.size());
    }
    out_.write_list_end();

    // end value write
    out_.write_list_end();
  }

private:
  std::string mapper_id_;
  std::list<typedbytes_opaque> keys_;
  std::vector<double> row_accumulator_;
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

  FullTSQRMap1 map(in, out, 5, 1);
  map.mapper();
  return 0;

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
