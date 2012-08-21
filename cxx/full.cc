#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string>

#include <algorithm>
#include <vector>

#include "typedbytes.h"
#include "sparfun_util.h"


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

int main(int argc, char **argv) {
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);
    
  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

    
}
