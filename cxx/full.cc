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
    read_opaque(key);
    std::vector<double> row;
    read_full_row(row);
    ncols = row.size();
    hadoop_message("matrix size: %zi ncols, up to %i localrows\n", 
		   ncols, blocksize * ncols);
    alloc(blocksize * ncols, ncols); 
    add_row(row);
  }

  // TODO(arbenson): QR decomposition that gets Q and R.
  void compress();
    
  // TODO(arbenson): Write Q and R to two different files.
  void output();

private:
  std::string mapper_id_;
  std::list<typedbytes_opaque> keys_;
  std::vector<double> Q_;
  std::vector<double> R_;
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
