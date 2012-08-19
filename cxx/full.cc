#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string>

#include <algorithm>
#include <vector>

#include "typedbytes.h"
#include "sparfun_util.h"

static std::string pseudo_uuid() {
  size_t len = 32;
  std::string uuid;
  uuid.reserve(len);
  for (int i = 0; i < len / sizeof(unsigned int); ++i) {
    snprintf(&uuid[i * sizeof(unsigned int)], sizeof(unsigned int),
             "%x", sf_rand_uint());
  }
  return uuid;
}

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
    read_key_val_pair(in_, key, row);
    num_cols_ = row.size();
    hadoop_message("matrix size: %zi num_cols", num_cols_);
    collect(key, row);
  }


  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {
    keys_.push_back(key);
    for (size_t i = 0; i < value.size(); ++i) {
      row_accumulator_.push_back(&value[i]);
    }
    ++num_rows_;
  }

  // TODO(arbenson): Write Q and R to two different files.
  void output() {
    double *R_matrix = malloc(num_cols_ * num_cols_ * sizeof(double));
    assert(R);
    // lapack_qr(row_accumulator_, R_matrix)

    // TODO(arbenson): how to write to multiple output files with Hadoop streaming
    // output R
    out_.write_string_stl(mapper_id_);
    out_.write_list_start();
    for (int i = 0; i < num_cols_; ++i) {
      for (int j = 0; j < num_cols_; ++j) {
        out_.write_double(R_matrix[i + j * num_rows_]);
      }
    }
    out.write_list_end();

    // output (Q, keys)
    // write key
    out_.write_string_stl(mapper_id_);

    // start value write
    out_.write_list_start();

    // write Q
    out_.write_list_start();
    for (size_t i = 0; i < row_accumulator_.size(); ++i) {
      out_.write_double(row_accumulator_[i]);
    }
    out_.write_list_end();

    // write keys
    out_.write_list_start();
    for (std::list<typedbytes_opaque>::iterator it = keys_.begin();
         it != keys_.end(); ++it) {
      typedbytes_opaque key = *it;
      write_opaque_type(&key[0], key.size());
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

/*
class FullTSQRReduce2: public MatrixHandler {
  
  void close_R() {
    std::list<double *> data;
    for (key in r_data_) {
      data.push_back(r_data_[key]);
      key_order_.append(key);
    }
    // QR on data
    // assign Q2
    // assign R_final
    if (compute_svd_) {
      // compute SVD
    }
  }

  void close_Q() {
    size_t ind = 0;
    size_t key_ind = 0;
    size_t num_rows = Q2_.length();
    size_t rows_to_read = num_rows / key_order_.size();
    for(std::list<double *>::iterator it = Q2_.begin();
        it != Q2_.end; ++it) {
      std::list<double *> local_Q;
      double *row = *it;
      local_Q.push_back(row);
      ++ind;
      if (ind == rows_to_read) {
        int key = key_order_[key_ind];
        // yield key, local_Q
        ++key_ind;
        ind = 0;
      }
    }
  }

private:
  std::map<typedbytes_opaque, double *matrix> r_data_;
  std::vector<typedbytes_opaque> key_order_;
  std::list<double *> Q2_;
  bool compute_svd_;
};
*/
