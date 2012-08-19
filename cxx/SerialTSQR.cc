class SerialTSQR : public MatrixHandler {
public:
  SerialTSQR(TypedBytesInFile& in, TypedBytesOutFile& out,
	    size_t blocksize, size_t rows_per_record)
    : MatrixHandler(in, out, blocksize, rows_per_record) {}
  virtual ~SerialTSQR() {}
    
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
      for (size_t j = 0; j<num_cols_; ++j) {
	out_.write_double(local_matrix_[i + j * num_rows_]);
      }
      out_.write_list_end();
    }
  }
};
