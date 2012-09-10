#include <vector>

#include "mrmc.h"
#include "typedbytes.h"

void MatrixHandler::read_full_row(std::vector<double>& row) {
  row.clear();
  TypedBytesType code = in_.next_type();
  typedbytes_length len;
  TypedBytesType nexttype;
  switch (code) {
  case TypedBytesVector:
    len = in_.read_typedbytes_sequence_length();
    row.reserve((size_t)len);
    for (size_t i = 0; i < (size_t) len; ++i) {
      nexttype = in_.next_type();
      if (in_.can_be_double(nexttype)) {
	row.push_back(in_.convert_double());
      } else {
	hadoop_error("row %zi, col %zi has a non-double-convertable type\n",
		     num_total_rows_, row.size());
      }
    }
    break;
  case TypedBytesByteSequence:
    len = in_.read_byte_sequence_length();
    row.resize((size_t) len / sizeof(double));
    in_.read_byte_sequence((unsigned char *) &row[0], len);
    break;
  case TypedBytesList:
    hadoop_message("TypedBytesList!\n");
    nexttype = in_.next_type();
    while (nexttype != TypedBytesListEnd) {
      if (in_.can_be_double(nexttype)) {
	row.push_back(in_.convert_double());
      } else {
	hadoop_message("row has a non-double-convertable type!!\n");
	hadoop_error("row %zi, col %zi has a non-double-convertable type\n",
		     num_total_rows_, row.size());
      }
      nexttype = in_.next_type();
    }
    break;
  case TypedBytesString:
    len = in_.read_string_length();
    row.resize(len / 8);
    in_.read_string_data((unsigned char *) &row[0], (size_t) len);
    break;
  default:
    hadoop_error("row %zi is an unknown type (code is: %d)\n",
		 num_total_rows_, code);
  }
}

bool MatrixHandler::read_key_val_pair(typedbytes_opaque& key,
				      std::vector<double>& value) {
  if (!in_.read_opaque(key)) {
    return false;
  }
  read_full_row(value); 
  return true;
}

void MatrixHandler::mapper() {
  std::vector<double> row;
  first_row();
  while (!feof(in_.get_stream())) {
    typedbytes_opaque key;
    if (!read_key_val_pair(key, row)) {
      if (feof(in_.get_stream())) {
	break;
      } else {
	hadoop_error("invalid key: row %i\n", num_total_rows_);
      }
    }
    collect(key, row);
  }
  hadoop_status("final output");
  output();
}
    
// Allocate the local matrix and set to zero
void MatrixHandler::alloc(size_t num_rows, size_t num_cols) {
  local_matrix_.resize(num_rows * num_cols);
  for (size_t i = 0; i < num_rows * num_cols; ++i) {
    local_matrix_[i] = 0.;
  }
  num_rows_ = num_rows;
  num_cols_ = num_cols;
  num_local_rows_ = 0;
}    
    
// Handle the first input row.  We use the first row to gather data
// about the matrix.
void MatrixHandler::first_row() {
  typedbytes_opaque key;
  std::vector<double> row;
  read_key_val_pair(key, row);
  // TODO(arbenson) check for error here
  num_cols_ = row.size();
  hadoop_message("matrix size: %zi columns, up to %i localrows\n", 
		 num_cols_, blocksize_ * num_cols_);
  if (num_cols_ == 0) {
    hadoop_message("no data received on this task\n");
    return;
  }
  alloc(blocksize_ * num_cols_, num_cols_);
  add_row(row);
}
    
// read in a row and add it to the local matrix
void MatrixHandler::add_row(const std::vector<double>& row) {
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
