#include "mrmc.h"
#include "typedbytes.h"
#include "tsqr_util.h"
#include "sparfun_util.h"

#include <stdio.h>

#include <vector>
#include <string>
#include <list>

std::string FullTSQRMap1::pseudo_uuid() {
  char buf[32];
  snprintf(buf, sizeof(buf), "%x%x%x%x",
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000),
	   (unsigned int) sf_randint(0, 2000000000));
  std::string uuid(buf);
  return uuid;
}
 
void FullTSQRMap1::first_row() {
  typedbytes_opaque key;
  std::vector<double> row;
  read_key_val_pair(key, row);
  num_cols_ = row.size();
  hadoop_message("matrix size: %zi\n", num_cols_);
  collect(key, row);
}

void FullTSQRMap1::collect(typedbytes_opaque& key, std::vector<double>& value) {
  keys_.push_back(key);
  for (size_t i = 0; i < value.size(); ++i) {
    row_accumulator_.push_back(value[i]);
  }
  ++num_rows_;
}

void FullTSQRMap1::output() {
  // num_cols_ is 0 if the task did not receive any data
  if (num_cols_ == 0) {
    return;
  }
  // Storage for R
  double *R_matrix = (double *) malloc(num_cols_ * num_cols_ * sizeof(double));
  assert(R_matrix);
  size_t num_rows = row_accumulator_.size() / num_cols_;
  hadoop_message("nrows: %d, ncols: %d\n", num_rows, num_cols_);
  // lapack is column major, unfortunately
  double *matrix_copy = (double *) malloc(num_rows * num_cols_ * sizeof(double));
  assert(matrix_copy);
  row_to_col_major(&row_accumulator_[0], matrix_copy, num_rows, num_cols_);
  row_accumulator_.clear();
  lapack_full_qr(matrix_copy, R_matrix, num_rows, num_cols_, num_rows);

  // output R
  out_.write_list_start();
  // Specify output file
  std::string output_file = "R_" + mapper_id_;
  out_.write_string_stl(output_file);
  // Specify actual key
  out_.write_string_stl(mapper_id_);
  out_.write_list_end();

  hadoop_message("Output: R");
  out_.write_byte_sequence((unsigned char *) R_matrix,
			   num_cols_ * num_cols_ * sizeof(double));


  hadoop_message("Output: Q");

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

  out_.write_byte_sequence((unsigned char *) matrix_copy,
			   num_rows * num_cols_ * sizeof(double));

  hadoop_message("Output: keys");
  size_t total_key_size = 0;
  for (std::list<typedbytes_opaque>::iterator it = keys_.begin();
       it != keys_.end(); ++it) {
    total_key_size += it->size();
  }
  // We also need to account for approximately the size to store the
  // lengths.  We are basically trying to accomplish a Python pickling
  // of this data.
  total_key_size += 4 * keys_.size();
  typedbytes_opaque key_holder;
  key_holder.reserve(total_key_size);

  assert(keys_.size() == num_rows);
  for (std::list<typedbytes_opaque>::iterator it = keys_.begin();
       it != keys_.end(); ++it) {
    typedbytes_opaque key = *it;
    char buf[10];
    snprintf(buf, sizeof(buf), "%zu", key.size());
    for (size_t i = 0; i < strlen(buf); ++i) {
      key_holder.push_back(buf[i]);
    }
    key_holder.push_back('\0');
    for (size_t i = 0; i < key.size(); ++i) {
      key_holder.push_back(key[i]);
    }
  }

  out_.write_byte_sequence(&key_holder[0], key_holder.size());

  // end value write
  out_.write_list_end();
}

void FullTSQRReduce2::first_row() {
  hadoop_message("reading first row!\n");
  typedbytes_opaque key;
  std::vector<double> row;
  read_key_val_pair(key, row);
  hadoop_message("matrix size: %zi\n", num_cols_);
  collect(key, row);
}

void FullTSQRReduce2::collect(typedbytes_opaque& key, std::vector<double>& value) {
  keys_.push_back(key);
  for (size_t i = 0; i < value.size(); ++i) {
    row_accumulator_.push_back(value[i]);
  }
  num_rows_ += num_cols_;
}

void FullTSQRReduce2::output() {
  // Storage for R
  double *R_matrix = (double *) malloc(num_cols_ * num_cols_ * sizeof(double));
  assert(R_matrix);
  size_t num_rows = row_accumulator_.size() / num_cols_;
  hadoop_message("nrows: %d, ncols: %d\n", num_rows, num_cols_);

  lapack_full_qr(&row_accumulator_[0], R_matrix, num_rows, num_cols_, num_rows);

  // output R
  for (size_t i = 0; i < num_cols_; ++i) {
    out_.write_list_start();
    // Specify output file
    std::string output_file = "R_final";
    out_.write_string_stl(output_file);
    // Specify actual key
    out_.write_int((int) i);
    out_.write_list_end();

    // Write the value
    out_.write_list_start();
    for (size_t j = 0; j < num_cols_; ++j)
      out_.write_double(R_matrix[j + i * num_cols_]);
    out_.write_list_end();
  }

  // output Q
  size_t ind = 0;
  for (std::list<typedbytes_opaque>::iterator it = keys_.begin();
       it != keys_.end(); ++it) {
    // Specify output file
    out_.write_list_start();
    std::string output_file = "Q2";
    out_.write_string_stl(output_file);
    // Specify actual key
    typedbytes_opaque tb_key = *it;
    std::string key((const char *) &tb_key[0], tb_key.size());
    out_.write_string_stl(key);
    out_.write_list_end();

    // write value
    out_.write_list_start();
    for (size_t j = 0; j < num_cols_ * num_cols_; ++j) {
      out_.write_double(row_accumulator_[ind]);
      ++ind;
    }
    out_.write_list_end();
  }
}

bool FullTSQRMap3::read_key_val_pair(typedbytes_opaque& key,
                                     std::vector<double>& value,
                                     std::list<typedbytes_opaque>& key_list) {
  if (!in_.read_opaque(key)) {
    return false;
  }
  TypedBytesType code = in_.next_type();
  if (code != TypedBytesList)
    hadoop_error("expected a value list!\n");

  read_full_row(value);
  code = in_.next_type();
  if (code != TypedBytesByteSequence)
    hadoop_error("expected key sequence!\n");

    
  typedbytes_length  len = in_.read_byte_sequence_length();
  std::vector<unsigned char> keys;
  keys.resize((size_t) len);
  in_.read_byte_sequence((unsigned char *) &keys[0], len);
    
  unsigned char *prev = &keys[0];
  unsigned char *next = &keys[0];
  size_t chars_skipped = 0;
  while (chars_skipped < (size_t) len) {
    while (*next++ != '\0') ;
    chars_skipped += next - prev;
    size_t next_len = (size_t) atoi((const char *)prev);
    typedbytes_opaque curr_key;
    curr_key.resize(next_len);
    memcpy(&curr_key[0], next, next_len);
    key_list.push_back(curr_key);
    chars_skipped += next_len;
    next += next_len;
    prev = next;
  }
    
  code = in_.next_type();
  if (code != TypedBytesListEnd)
    hadoop_error("expected the end of the key list!\n");

  return true;
}

void FullTSQRMap3::collect(typedbytes_opaque& key, std::vector<double>& value,
			   std::list<typedbytes_opaque>& key_list) {
  std::string str_key((const char *) &key[0], key.size());
  Q_matrices_[str_key] = value;
  keys_[str_key] = key_list;
}

void FullTSQRMap3::mapper() {
  while (!feof(in_.get_stream())) {
    typedbytes_opaque key;
    std::vector<double> row;
    std::list<typedbytes_opaque> string_keys;
    if (!read_key_val_pair(key, row, string_keys)) {
      if (feof(in_.get_stream())) {
	break;
      } else {
	hadoop_error("invalid key: row %i\n", num_total_rows_);
      }
    }
    collect(key, row, string_keys);
  }
  hadoop_status("final output");
  output();
}

void FullTSQRMap3::output() {
  FILE *f = fopen(Q2_path_.c_str(), "r");
  assert(f);
  char b[262144];
  while (fgets(b, sizeof(b), f)) {
    char *buf = b;
    size_t i;
    while (*buf != '\0' && *buf++ != '(') ;
    if (*buf == '\0')
      hadoop_error("could not find key while parsing matrix\n");

    for (i = 0; buf[i] != '\0' && buf[i] != ')'; ++i) ;
    if (buf[i] == '\0')
      hadoop_error("could not find key while parsing matrix\n");

    std::string key((const char *) buf, i);
    std::map<std::string, std::vector<double>>::iterator Q_it =
      Q_matrices_.find(key);
    if (Q_it == Q_matrices_.end())
      continue;

    buf += i + 1;
    while (*buf != '\0' && *buf++ != '[') ;
    if (*buf == '\0')
      hadoop_error("could not find value while parsing matrix\n");

    std::vector<double> value;
    value.reserve(num_cols_ * num_cols_);
    double val;
    while (true) {
      for (i = 0; buf[i] != '\0' && buf[i] != ',' && buf[i] != ']'; ++i) ;
      if (buf[i] == '\0') {
	hadoop_error("could not find value\n");
      } else if (buf[i] == ',') {
	if (sscanf(buf, "%lg,", &val) != 1)
	  hadoop_error("non-double in value\n");
	value.push_back(val);
	buf += i + 1;
	// skip whitespace
	++buf;
      } else {
	if (sscanf(buf, "%lg]", &val) != 1)
	  hadoop_error("non-double in value\n");
	value.push_back(val);
	break;
      }
    }
    assert(value.size() == num_cols_ * num_cols_);
    handle_matmul(key, value);
  }
}

void FullTSQRMap3::handle_matmul(std::string& key, std::vector<double>& Q2) {
  std::map<std::string, std::vector<double>>::iterator Q_it =
    Q_matrices_.find(key);
  assert(Q_it != Q_matrices_.end());
    
  std::vector<double>& Q1(Q_it->second);

  std::map<std::string, std::list<typedbytes_opaque>>::iterator key_it =
    keys_.find(key);
  assert(key_it != keys_.end());
  std::list<typedbytes_opaque>& key_output(key_it->second);
  if (Q1.size() / num_cols_ != key_output.size())
    hadoop_message("num rows: %d, keys: %d\n", Q1.size() / num_cols_, key_output.size());
  assert(Q1.size() / num_cols_ == key_output.size());

  size_t num_rows = Q1.size() / num_cols_;

  double *C = (double *) malloc (Q1.size() * sizeof(double));
  assert(C);
  lapack_tsmatmul(&Q1[0], num_rows, num_cols_, &Q2[0], num_cols_, C);

  col_to_row_major(C, &Q1[0], num_rows, num_cols_);
  free(C);

  double *out = &Q1[0];
  while (!key_output.empty()) {
    typedbytes_opaque curr_key = key_output.front();
    out_.write_byte_sequence(&curr_key[0], curr_key.size());
    out_.write_byte_sequence((unsigned char *) out, num_cols_ * sizeof(double));
    out += num_cols_;
    key_output.pop_front();
  }
}


