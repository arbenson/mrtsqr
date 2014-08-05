/**
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
*/

#include <assert.h>
#include <vector>

#include "mrmc.h"
#include "sparfun_util.h"
#include "tsqr_util.h"

void AtA::collect(typedbytes_opaque& key, std::vector<double>& value) {
  add_row(value);
  if (num_local_rows_ >= num_rows_) {
    compress();
    hadoop_counter("compressions", 1);
  }
}

void AtA::compress() {
  double t0 = sf_time();
  if (local_AtA_ == NULL) {
    local_AtA_ = (double *) calloc(num_cols_ * num_cols_, sizeof(double));
    assert(local_AtA_);
  }
  if (lapack_syrk(&local_matrix_[0], local_AtA_, num_rows_, num_cols_,
		  num_local_rows_)) {
    incr_lapack_time(sf_time() - t0);
  } else {
    hadoop_error("lapack error\n");
  }
  num_local_rows_ = 0;
}
    
void AtA::output() {
  for (size_t i = 0; i < num_cols_; ++i) {
    out_.write_int(i);
    out_.write_list_start();
    for (size_t j = 0; j < num_cols_; ++j)
      out_.write_double(local_AtA_[i + j * num_rows_]);
    out_.write_list_end();
  }
}

bool RowSum::read_key_val_pair(int *key, std::vector<double>& value) {
  TypedBytesType type = in_.next_type();
  if (type != TypedBytesInteger) {
    hadoop_message("invalid key, TypedBytes code: %d (skipping) \n", type);
    in_.skip_next();
    return false;
  }
  *key = in_.read_int();
  read_full_row(value); 
  return true;
}

void RowSum::mapper() {
  std::vector<double> row;
  first_row();
  while (!feof(in_.get_stream())) {
    int key = -1;
    if (!read_key_val_pair(&key, row)) {
      if (feof(in_.get_stream())) {
	break;
      } else {
	hadoop_error("invalid key: row %i\n", num_total_rows_);
      }
    }
    collect_int_key(key, row);
  }
  hadoop_status("final output");
  output();
}

void RowSum::first_row() {
  int row_index = -1;
  std::vector<double> row;
  read_key_val_pair(&row_index, row);
  num_cols_ = row.size();
  rows_.resize(num_cols_);
  used_.resize(num_cols_);
  assert(row_index < (int) num_cols_);
  for (int i = 0; i < (int) num_cols_; ++i) {
    rows_[i] = (double *) calloc(num_cols_, sizeof(double));
    used_[i] = false;
  }
  hadoop_message("matrix size: %zi columns\n", num_cols_);
  collect_int_key(row_index, row);
}
    
void RowSum::collect_int_key(int key, std::vector<double>& value) {
  assert(value.size() == num_cols_);
  assert((size_t) key < num_cols_);
  used_[key] = true;
  double t0 = sf_time();
  lapack_daxpy(num_cols_, rows_[key], &value[0]);
  incr_lapack_time(sf_time() - t0);
}

void RowSum::output() {
  for (size_t i = 0; i < rows_.size(); ++i) {
    if (!used_[i])
      continue;
    out_.write_int(i);
    out_.write_list_start();
    for (size_t j = 0; j < num_cols_; ++j)
      out_.write_double(rows_[i][j]);
    out_.write_list_end();
  }
}

void Cholesky::output() {
  // all data needs to be on this task
  for (size_t i = 0; i < used_.size(); ++i)
    assert(used_[i]);
  double *mat = (double *) malloc(num_cols_ * num_cols_ * sizeof(double));
  assert(mat);

  for (size_t i = 0; i < num_cols_; ++i) {
    double *curr_row = rows_[i];
    for (size_t j = 0; j < num_cols_; ++j)
      mat[i * num_cols_ + j] = curr_row[j];
  }

  // call Cholesky once
  double t0 = sf_time();
  lapack_chol(mat, (int) num_cols_);
  incr_lapack_time(sf_time() - t0);

  for (size_t i = 0; i < num_cols_; ++i) {
    out_.write_int(i);
    out_.write_list_start();
    for (size_t j = 0; j < num_cols_; ++j)
      out_.write_double(j <=i ? mat[i * num_cols_ + j] : 0.0);
    out_.write_list_end();
  }
}
