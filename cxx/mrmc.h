#ifndef MRTSQR_CXX_MRMC_H_
#define MRTSQR_CXX_MRMC_H_

#include "typedbytes.h"
#include "tsqr_util.h"

#include <algorithm>
#include <list>
#include <map>
#include <string>
#include <vector>

#include <time.h>

class MatrixHandler {
public:
  MatrixHandler(TypedBytesInFile& in, TypedBytesOutFile& out,
                size_t blocksize, size_t rows_per_record)
    : in_(in), out_(out),
      blocksize_(blocksize), rows_per_record_(rows_per_record),
      num_cols_(0), num_rows_(0), num_total_rows_(0) {}

  ~MatrixHandler() {}

  void read_full_row(std::vector<double>& row);

  bool read_key_val_pair(typedbytes_opaque& key,
                         std::vector<double>& value);

  virtual void mapper();
    
  // Allocate the local matrix and set to zero
  virtual void alloc(size_t num_rows, size_t num_cols);

  // Handle the first input row.  We use the first row to gather data
  // about the matrix.
  virtual void first_row();
    
  // read in a row and add it to the local matrix
  virtual void add_row(const std::vector<double>& row);

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) = 0;
  virtual void output() = 0;

  // add time (given in seconds) to the Hadoop counter
  void incr_lapack_time(double time) {
    hadoop_counter("lapack time (millisecs)", (int) (time * 1000.));
  }

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

  void collect(typedbytes_opaque& key, std::vector<double>& value);
  // compress the local QR factorization
  void compress();
  // Output the matrix with random keys for the rows.
  void output();
};

class AtA : public MatrixHandler {
public:
  AtA(TypedBytesInFile& in, TypedBytesOutFile& out,
      size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {
    local_AtA_ = NULL;
  }

  // Call syrk and store the result in local AtA computation
  void compress();
  // Output the matrix with key equal to row number
  void output();
  void collect(typedbytes_opaque& key, std::vector<double>& value);
  
private:
  double *local_AtA_;
};

class RowSum : public MatrixHandler {
public:
  RowSum(TypedBytesInFile& in, TypedBytesOutFile& out,
         size_t rows_per_record)
    : MatrixHandler(in, out, -1, rows_per_record) {}

  bool read_key_val_pair(int *key, std::vector<double>& value);
  void output();
  void first_row();
  void add_row(const std::vector<double>& row, int row_index);
  void collect(typedbytes_opaque& key, std::vector<double>& value) {}
  void collect_int_key(int key, std::vector<double>& value);
  void mapper();
  
  std::vector<double*> rows_;
  std::vector<bool> used_;
};

class Cholesky : public RowSum {
public:
  Cholesky(TypedBytesInFile& in, TypedBytesOutFile& out,
           size_t rows_per_record)
    : RowSum(in, out, rows_per_record) {}

  // Computes Cholesky decomposition and outputs R
  void output();
};

class DirTSQRMap1 : public MatrixHandler {
public:
  DirTSQRMap1(TypedBytesInFile& in, TypedBytesOutFile& out,
               size_t rows_per_record)
    : MatrixHandler(in, out, -1, rows_per_record) {
    mapper_id_ = pseudo_uuid();
    num_cols_ = 0;
  }
  virtual ~DirTSQRMap1() {}

  std::string pseudo_uuid();
  void first_row();
  void collect(typedbytes_opaque& key, std::vector<double>& value);
  void output();

private:
  std::string mapper_id_;
  std::list<typedbytes_opaque> keys_;
  std::vector<double> row_accumulator_;
};

class DirTSQRReduce2: public MatrixHandler {
public:
  DirTSQRReduce2(TypedBytesInFile& in, TypedBytesOutFile& out,
                  size_t rows_per_record, size_t num_cols)
    : MatrixHandler(in, out, -1, rows_per_record) {
    num_cols_ = num_cols;
  }

  virtual ~DirTSQRReduce2() {}
  
  void first_row();
  void collect(typedbytes_opaque& key, std::vector<double>& value);
  void output();

private:
  std::vector<double> row_accumulator_;
  std::list<typedbytes_opaque> keys_;
};

class DirTSQRMap3: public MatrixHandler {
public:
  DirTSQRMap3(TypedBytesInFile& in, TypedBytesOutFile& out,
               size_t rows_per_record, size_t num_cols)
    : MatrixHandler(in, out, -1, rows_per_record_) {
    num_cols_ = num_cols;
    // TODO(arbenson): make the Q2 path a constructor argument
    Q2_path_ = "Q2.txt.out";
  }

  bool read_key_val_pair(typedbytes_opaque& key,
                         std::vector<double>& value,
                         std::list<typedbytes_opaque>& key_list);
  void collect(typedbytes_opaque& key, std::vector<double>& value,
               std::list<typedbytes_opaque>& key_list);
  void mapper();
  void output();
  void collect(typedbytes_opaque& key, std::vector<double>& value) {}

private:
  std::map<std::string, std::vector<double>> Q_matrices_;
  std::map<std::string, std::list<typedbytes_opaque>> keys_;
  std::string Q2_path_;

  void handle_matmul(std::string& key, std::vector<double>& Q2);
};

#endif  // MRTSQR_CXX_MRMC_H_

