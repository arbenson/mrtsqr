// Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
// David F. Gleich
// Austin R. Benson

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>

#include <algorithm>
#include <list>
#include <map>
#include <string>
#include <vector>

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
      for (size_t i = 0; i < (size_t) len; ++i) {
        TypedBytesType nexttype = in_.next_type();
        if (in_.can_be_double(nexttype)) {
          row.push_back(in_.convert_double());
        } else {
          hadoop_error("row %zi, col %zi has a non-double-convertable type\n",
                       num_total_rows_, row.size());
        }
      }
    } else if (code == TypedBytesList) {
      hadoop_message("TypedBytesList!\n");
      TypedBytesType nexttype = in_.next_type();
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
    } else if (code == TypedBytesString) {
      typedbytes_length len = in_.read_string_length();
      row.resize(len / 8);
      in_.read_string_data((unsigned char *) &row[0], (size_t) len);
    } else {
      hadoop_error("row %zi is a not a list, vector, or string (code is: %d)\n",
                   num_total_rows_, code);
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
  virtual void alloc(size_t num_rows, size_t num_cols) {
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

class FullTSQRMap1 : public MatrixHandler {
public:
  FullTSQRMap1(TypedBytesInFile& in_, TypedBytesOutFile& out_,
               size_t blocksize_, size_t rows_per_record_)
    : MatrixHandler(in_, out_, blocksize_, rows_per_record_) {
    mapper_id_ = pseudo_uuid();
    num_cols_ = 0;
  }
  virtual ~FullTSQRMap1() {}

  void first_row() {
    typedbytes_opaque key;
    std::vector<double> row;
    read_key_val_pair(key, row);
    num_cols_ = row.size();
    hadoop_message("matrix size: %zi\n", num_cols_);
    collect(key, row);
  }

  void collect(typedbytes_opaque& key, std::vector<double>& value) {
    keys_.push_back(key);
    for (size_t i = 0; i < value.size(); ++i) {
      row_accumulator_.push_back(value[i]);
    }
    ++num_rows_;
  }

  void output() {
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
    double *matrix_copy = (double *) malloc(num_rows_ * num_cols_ * sizeof(double));
    assert(matrix_copy);
    for (size_t i = 0; i < num_rows_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        matrix_copy[i + j * num_rows] = row_accumulator_[i * num_cols_ + j];
      }
    }
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
    out_.write_list_start();
    for (size_t i = 0; i < num_cols_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        out_.write_double(R_matrix[j + i * num_cols_]);
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
    for (size_t i = 0; i < num_rows_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        row_accumulator_[i * num_cols_ + j] = matrix_copy[i + j * num_rows];
      }
    }
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
      // We write this out as a string
      std::string str_key((const char *) &key[0], key.size());
      out_.write_string_stl(str_key);
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

class FullTSQRReduce2: public MatrixHandler {
public:
  FullTSQRReduce2(TypedBytesInFile& in_, TypedBytesOutFile& out_,
                  size_t blocksize_, size_t rows_per_record_, size_t num_cols)
    : MatrixHandler(in_, out_, blocksize_, rows_per_record_) {
    num_cols_ = num_cols;
  }

  virtual ~FullTSQRReduce2() {}
  
  void first_row() {
    hadoop_message("reading first row!\n");
    typedbytes_opaque key;
    std::vector<double> row;
    read_key_val_pair(key, row);
    hadoop_message("matrix size: %zi\n", num_cols_);
    collect(key, row);
  }

  void collect(typedbytes_opaque& key, std::vector<double>& value) {
    keys_.push_back(key);
    for (size_t i = 0; i < value.size(); ++i) {
      row_accumulator_.push_back(value[i]);
    }
    num_rows_ += num_cols_;
  }

  void output() {
    // Storage for R
    double *R_matrix = (double *) malloc(num_cols_ * num_cols_ * sizeof(double));
    assert(R_matrix);
    size_t num_rows = row_accumulator_.size() / num_cols_;
    hadoop_message("nrows: %d, ncols: %d\n", num_rows, num_cols_);

    double *matrix_copy = (double *) malloc(num_rows_ * num_cols_ * sizeof(double));
    assert(matrix_copy);
    for (size_t i = 0; i < num_rows_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        matrix_copy[i + j * num_rows] = row_accumulator_[i * num_cols_ + j];
      }
    }

    lapack_full_qr(matrix_copy, R_matrix, num_rows, num_cols_, num_rows);

    for (size_t i = 0; i < num_rows_; ++i) {
      for (size_t j = 0; j < num_cols_; ++j) {
        row_accumulator_[i * num_cols_ + j] = matrix_copy[i + j * num_rows];
      }
    }

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

private:
  std::vector<double> row_accumulator_;
  std::list<typedbytes_opaque> keys_;
};

class FullTSQRMap3: public MatrixHandler {
public:
  FullTSQRMap3(TypedBytesInFile& in_, TypedBytesOutFile& out_,
               size_t blocksize_, size_t rows_per_record_, size_t num_cols)
    : MatrixHandler(in_, out_, blocksize_, rows_per_record_) {
    num_cols_ = num_cols;
    Q2_path_ = "Q2.txt.out";
  }

  bool read_key_val_pair(typedbytes_opaque& key,
                         std::vector<double>& value,
                         std::list<std::string>& strings) {
    if (!in_.read_opaque(key)) {
      return false;
    }
    TypedBytesType code = in_.next_type();
    if (code != TypedBytesList) {
      hadoop_error("expected a value list!\n");
    }
    read_full_row(value); 
    code = in_.next_type();
    if (code != TypedBytesList) {
      hadoop_error("expected a key list!\n");
    }
    TypedBytesType nexttype = in_.next_type();
    while (nexttype != TypedBytesListEnd) {
      if (nexttype != TypedBytesString) {
        hadoop_error("expected a string key!\n");
      }
      typedbytes_length len = in_.read_string_length();
      std::string str(len, 0);
      in_.read_string_data((unsigned char *) &str[0], (size_t) len);
      strings.push_back(str);
      nexttype = in_.next_type();
    }
    nexttype = in_.next_type();
    if (nexttype != TypedBytesListEnd) {
      hadoop_error("expected the end of the value list!\n");
    }
    return true;
  }

  void collect(typedbytes_opaque& key, std::vector<double>& value,
               std::list<std::string>& strings) {
    std::string str_key((const char *) &key[0], key.size());
    Q_matrices_[str_key] = value;
    keys_[str_key] = strings;
  }

  virtual void mapper() {
    while (!feof(in_.get_stream())) {
      typedbytes_opaque key;
      std::vector<double> row;
      std::list<std::string> string_keys;
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

  void output() {
    FILE *f = fopen(Q2_path_.c_str(), "r");
    assert(f);
    char b[32768];
    while (fgets(b, sizeof(b), f)) {
      char *buf = b;
      fprintf(stderr, "%s", buf);
      size_t i;
      while (*buf != '\0' && *buf++ != '(') ;
      if (*buf == '\0')
        hadoop_error("could not find key while parsing matrix\n");

      for (i = 0; buf[i] != '\0' && buf[i] != ')'; ++i) ;
      if (buf[i] == '\0')
        hadoop_error("could not find key while parsing matrix\n");

      std::string key((const char *) buf, i);
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

  void handle_matmul(std::string& key, std::vector<double>& Q2) {
    std::map<std::string, std::vector<double>>::iterator Q_it =
      Q_matrices_.find(key);
    if (Q_it == Q_matrices_.end())
      return;
    
    std::vector<double>& Q1(Q_it->second);

    std::map<std::string, std::list<std::string>>::iterator key_it =
      keys_.find(key);
    assert(key_it != keys_.end());
    std::list<std::string>& key_output(key_it->second);
    assert(Q1.size() / num_cols_ == key_output.size());

    double *C= (double *) malloc (Q1.size() * sizeof(double));
    lapack_tsmatmul(&Q1[0], Q1.size() / num_cols_, num_cols_,
                    &Q2[0], num_cols_, C);

    size_t ind = 0;
    for (std::list<std::string>::iterator it = key_output.begin();
         it != key_output.end(); ++it) {
      out_.write_string_stl(*it);
      out_.write_list_start();
      for (size_t i = 0; i < num_cols_; ++i) {
        out_.write_double(C[ind++]);
      }
      out_.write_list_end();
    }
  }

  virtual void collect(typedbytes_opaque& key, std::vector<double>& value) {}

private:
  std::map<std::string, std::vector<double>> Q_matrices_;
  std::map<std::string, std::list<std::string>> keys_;
  std::string Q2_path_;
};

// TODO(arbenson): real command-line options
int main(int argc, char** argv) {  
  // initialize the random number generator
  unsigned long seed = sf_randseed();
  hadoop_message("seed = %u\n", seed);

  // create typed bytes files
  TypedBytesInFile in(stdin);
  TypedBytesOutFile out(stdout);

  int stage = 1;
  if (argc > 2) {
    stage = atoi(argv[2]);
  }
  size_t ncols = 10;
  if (stage == 1) {
    FullTSQRMap1 map(in, out, 5, 1);
    map.mapper();
  } else if (stage == 2) {
    FullTSQRReduce2 map(in, out, 5, 1, ncols);
    map.mapper();
  } else if (stage == 3) {
    FullTSQRMap3 map(in, out, 5, 1, ncols);
    map.mapper();
  }
  return 0;
}
