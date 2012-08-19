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
    read_full_row(in_, row);
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
    if (lapack_syrk(&local[0], local_AtA_, num_rows_, num_cols_, num_local_rows_)) {
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

private:
  double *local_AtA_;
};

class RowSum : MatrixHandler {
public:
  RowSum(TypedBytesInFile& in, TypedBytesOutFile& out,
	 size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {}

  int read_key() {
    TypedBytesType type = in.next_type();
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
    read_full_row(in_, row);
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
    add_row(value, (int) key);
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
