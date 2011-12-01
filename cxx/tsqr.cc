/**
 * @file tsqr.cc
 * Implement TSQR in C++ using atlas and hadoop streaming with typedbytes.
 * @author David F. Gleich
 */

/**
 * History
 * -------
 * :2011-01-28: Initial coding
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string>

#include <algorithm>
#include <vector>

#include "typedbytes.h"
#include "sparfun_util.h"

/** Write a message to stderr
 */
void hadoop_message(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end (args);
}

/** Write a status message to hadoop.
 *
 */
void hadoop_status(const char* format, ...) {
    va_list args;
    va_start (args, format);
    
    // output for hadoop
    fprintf(stderr, "reporter:status:");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    
    // also print to stderr
    fprintf(stderr, "status: ");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    
    va_end (args);
}

void hadoop_counter(const char* name, int val) {
    fprintf(stderr, "reporter:counter:Program,%s,%i\n", name, val);
}

extern "C" {
  void dgeqrf_(int *m, int *n, double *a, int *lda, double *tau,
	       double *work, int *lwork, int *info);
  void dsyrk_(char *uplo, char *trans, int *m, int *k, double *alpha,
              double *A, int *lda, double *beta, double *C, int *ldc);
  void daxpy_(int *n, double *alpha, double *x, int *incx, double *y, int *incy);
  void dpotrf_(char *uplo, int *n, double *a, int *lda, int *info);
}

/** Run a LAPACK daxpy
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 */
bool lapack_daxpy(int size, double *x, double *y)
{
  int incx = 1;
  int incy = 1;
  int n = size;
  double alpha = 1;
  daxpy_(&n, &alpha, x, &incx, y, &incy);
  return true;
}

/** Run a LAPACK Cholesky
 * @param A the matrix in lower triangular form
 * @param ncols the number of columns of A allocated
 */
bool lapack_chol(double *A, int ncols) {
  char uplo = 'L';  // store in lower triangular form
  int n = ncols;    // always just a square update
  int lda = ncols;  // always just a square update
  int info;         // store result;
  dpotrf_(&uplo, &n, A, &lda, &info);
  /*
  if (info != 0) {
    fprintf(stderr, "matrix is not positive definite!\n");
    exit(-1);
  }
  */
  return true;
}

/** Run a LAPACK syrk with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_syrk(double* A, double* C, size_t nrows, size_t ncols, size_t urows)
{
  char trans = 'T';       // specify type to be A^TA
  char uplo = 'U';        // specificy triangular part to be read for C
  int n = ncols;          // order of C
  int k = urows;          // number of rows in A to update
  double alpha = 1.0;     // multiplier of AtA
  double beta = 1.0;      // multiplier of C
  int lda = nrows;        // leading dimension of A
  int ldc = ncols;        // leading dimension of C

  dsyrk_(&uplo, &trans, &n, &k, &alpha, A, &lda, &beta, C, &ldc);
  return true;
}

/** Run a LAPACK qr with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_qr(double* A, size_t nrows, size_t ncols, size_t urows)
{
    int info = -1;
    int n = ncols;
    int m = urows;
    int stride = nrows;
    int minsize = std::min(urows,ncols);
    
    // allocate space for tau's
    std::vector<double> tau(minsize);
    
    // do a workspace query
    double worksize;
    int lworkq = -1;
    
    dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
    if (info == 0) {
        int lwork = (int)worksize;
        std::vector<double> work(lwork);
	dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
        if (info == 0) {
            // zero out the lower triangle of A
            size_t rsize = (size_t)minsize;
            for (size_t j=0; j<rsize; ++j) {
                for (size_t i=j+1; i<rsize; ++i) {
                    A[i+j*nrows] = 0.;
                }
            }
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

class SerialTSQR {
public:
    TypedBytesInFile& in;
    TypedBytesOutFile& out;
    
    size_t blocksize;
    size_t rows_per_record; // number of rows in one record (defaults to 1)
    size_t ncols; // the number of columns of the local matrix
    size_t nrows; // the maximum number of rows of the local matrix
    size_t currows; // the current number of local rows
    size_t totalrows; // the total number of rows processed
    double tempval;
    
    std::vector<double> local; // the local matrix
    SerialTSQR(TypedBytesInFile& in_, TypedBytesOutFile& out_,
	       size_t blocksize_, size_t rows_per_record_)
      : in(in_), out(out_),
      blocksize(blocksize_), rows_per_record(rows_per_record_),
      ncols(0), nrows(0), totalrows(0), 
      tempval(0.)
    {}
    
    /** Allocate the local matrix and set to zero
     * @param nr the number of rows
     * @param nc the number of cols
     */
    virtual void alloc(size_t nr, size_t nc) {
        local.resize(nr*nc);
        for (size_t i = 0; i<nr*nc; ++i) {
            local[i] = 0.;
        }
        nrows = nr;
        ncols = nc;
        currows = 0;
    }
    
    virtual void read_full_row(std::vector<double>& row)
    {
        TypedBytesType code=in.next_type();
        row.clear();
        if (code == TypedBytesVector) {
	  hadoop_message("code == TypedBytesVector\n");
            typedbytes_length len=in.read_typedbytes_sequence_length();
            row.reserve((size_t)len);
            for (size_t i=0; i<(size_t)len; ++i) {
                TypedBytesType nexttype=in.next_type();
                if (in.can_be_double(nexttype)) {
                    row.push_back(in.convert_double());
                } else {
                    fprintf(stderr, 
                        "error: row %zi, col %zi has a non-double-convertable type\n",
                        totalrows, row.size());
                    exit(-1);
                }
            }
        } else if (code == TypedBytesList) {
	    hadoop_message("code == TypedBytesList\n");
            TypedBytesType nexttype=in.next_type();
            while (nexttype != TypedBytesListEnd) {
                if (in.can_be_double(nexttype)) {
                    row.push_back(in.convert_double());
                } else {
             	    hadoop_message("row has a non-double-convertable type!!\n");
                    fprintf(stderr, 
                        "error: row %zi, col %zi has a non-double-convertable type\n",
                        totalrows, row.size());
                    exit(-1);
                }
                nexttype=in.next_type();
            }
        } else if (code == TypedBytesString) {
	  //hadoop_message("code == TypedBytesString\n");
          typedbytes_length len = in.read_string_length();
          row.resize(len/8);
          in.read_string_data((unsigned char *)&row[0], (size_t)len);
        } else {
	    hadoop_message("row is not a list, vector, or string\n");
	    hadoop_message("code is: %d\n", code);
            fprintf(stderr,
                "error: row %zi is a not a list, vector, or string\n", totalrows);
            exit(-1);
        }
    }
    
    /** Handle the first input row.
     * The first row of the input is special, and so we handle
     * it differently.
     */
    virtual void first_row()
    {
        in.skip_next(); // skip the key
        std::vector<double> row;
        read_full_row(row);
        ncols = row.size();
        hadoop_message("matrix size: %zi ncols, up to %i localrows\n", 
            ncols, blocksize*ncols);
        alloc(blocksize*ncols, ncols);
        add_row(row);
    }
    
    // read in a row and add it to the local matrix
    virtual void add_row(const std::vector<double>& row) {
      //hadoop_message("row.size(): %d, ncols: %d, currows: %d, nrows: %d\n", row.size(), ncols, currows, nrows);
        assert(row.size() == ncols);
        assert(currows < nrows);
        // store by column
        for (size_t k=0; k<rows_per_record; ++k) {
          size_t i = 0;
	  for (size_t j=0; j<ncols; ++j) {
            local[currows + j*nrows] = row[i];
            ++i;
	  }
	  // increment the number of local rows
	  currows++;
	  totalrows++;
	}
    }

    virtual void mapper() {
        std::vector<double> row;
        first_row(); // handle the first row
        while (!feof(in.stream)) {
            if (in.skip_next() == false) {
                if (feof(in.stream)) {
                    break;
                } else {
                    hadoop_message("invalid key: row %i\n", totalrows);
                    exit(-1);
                }
            }
            read_full_row(row);

            add_row(row);
            
            if (currows >= nrows) {
	        hadoop_message("about to compress\n");
                compress();
	        hadoop_message("compress appears successful\n");
                hadoop_counter("compress", 1);
            }
        }
        compress();
        hadoop_status("final output");
        output();
    }
    
  // compress the local factorization
  virtual void compress() = 0;
    
  // Output the matrix
  virtual void output() = 0;

};

class HouseTSQR : public SerialTSQR {
public:
    HouseTSQR(TypedBytesInFile& in_, TypedBytesOutFile& out_,
	       size_t blocksize_, size_t rows_per_record_)
      : SerialTSQR(in_, out_, blocksize_, rows_per_record_) {}
    virtual ~HouseTSQR() {}
    
    // compress the local QR factorization
    void compress() {
        // compute a QR factorization
        double t0 = sf_time();
        if (lapack_qr(&local[0], nrows, ncols, currows)) {
            double dt = sf_time() - t0;
            hadoop_counter("lapack time (millisecs)", (int)(dt*1000.));
        } else {
            hadoop_message("lapack error\n");
            exit(-1);
        }
        if (ncols < currows) {
            currows = ncols;
        }
    }
    
    
    // Output the matrix with random keys for the rows.
    void output() {
        for (size_t i=0; i<currows; ++i) {
            int rand_int = sf_randint(0,2000000000);
            out.write_int(rand_int);
            //out.write_vector_start(ncols);
            out.write_list_start();
            for (size_t j=0; j<ncols; ++j) {
                out.write_double(local[i+j*nrows]);
            }
            out.write_list_end();
        }
    }
};

class Cholesky : public SerialTSQR {
public:
    Cholesky(TypedBytesInFile& in_, TypedBytesOutFile& out_,
	   size_t blocksize_, size_t rows_per_record_)
      : SerialTSQR(in_, out_, blocksize_, rows_per_record_) {}

  int read_key() {
    TypedBytesType type = in.next_type();
    if (type != TypedBytesInteger) {
      hadoop_message("invalid key, TypedBytes code: %d (skipping) \n",type);
      in.skip_next();
      return -1;
    }
    return in.read_int();
  }
  
  /** Handle the first input row.
   * The first row of the input is special, and so we handle
   * it differently.
   */
  void first_row() {
    int row_index = read_key();
    std::vector<double> row;
    read_full_row(row);
    ncols = row.size();
    rows_.resize(ncols);
    for (int i = 0; i < (int)ncols; ++i) {
      rows_[i] = NULL;
    }
    assert(row_index < (int)ncols);
    hadoop_message("matrix size: %zi ncols, up to %i localrows\n", 
		   ncols, blocksize*ncols);
    add_row(row, row_index);
  }
    
  // read in a row and add it to the local matrix
  void add_row(const std::vector<double>& row, int row_index) {
    double *new_row = (double *)malloc(ncols*sizeof(double));
    for (int i = 0; i < (int)ncols; ++i) {
      new_row[i] = row[i];
    }
    rows_[row_index] = new_row;
  }

  void reducer() {
    std::vector<double> row;
    first_row(); // handle the first row
    while (!feof(in.stream)) {
      int row_index = read_key();
      if (row_index == -1) continue;
      read_full_row(row);
      add_row(row, row_index);
    }
    hadoop_status("final output");
    output();
  }

  void compress() {}
  
  // Output the row sums
  void output() {
    int dim = (int)ncols;
    double *A = (double *)malloc(dim*dim*sizeof(double));
    assert(A);
    // copy matrix and check that all rows were filled
    for (int i = 0; i < dim; ++i) {
      double *curr_row = rows_[i];
      assert(curr_row);
      for (int j = 0; j < dim; ++j) {
	A[i*dim + j] = curr_row[j];
      }
      free(curr_row);
    }

    // call Cholesky once
    double t0 = sf_time();
    lapack_chol(A, dim);
    double dt = sf_time() - t0;
    hadoop_counter("lapack time (millisecs)", (int)(dt*1000.));

    for (int i = 0; i < dim; ++i) {
      out.write_int(i);
      out.write_list_start();
      for (int j = 0; j < dim; ++j) {
	if (j <= i) {
	  out.write_double(A[i*dim + j]);
	} else{
	  out.write_double(0.0);
	}
      }
      out.write_list_end();
    }
  }

private:
  std::vector<double*> rows_;
};

class AtA : public SerialTSQR {
public:
    AtA(TypedBytesInFile& in_, TypedBytesOutFile& out_,
        size_t blocksize_, size_t rows_per_record_)
      : SerialTSQR(in_, out_, blocksize_, rows_per_record_) {
      local_AtA_ = NULL;
    }

    // Call syrk and store the result in local AtA computation
    void compress() {
        double t0 = sf_time();
	if (!local_AtA_) {
	  local_AtA_ = (double *)calloc(ncols*ncols, sizeof(double));
	  assert(local_AtA_);
	}
        if (lapack_syrk(&local[0], local_AtA_, nrows, ncols, currows)) {
            double dt = sf_time() - t0;
            hadoop_counter("lapack time (millisecs)", (int)(dt*1000.));
        } else {
            hadoop_message("lapack error\n");
            exit(-1);
        }
	currows = 0;
    }
    
    // Output the matrix with key equal to row number
    void output() {
        for (size_t i=0; i<ncols; ++i) {
            out.write_int(i);
            out.write_list_start();
            for (size_t j=0; j<ncols; ++j) {
                out.write_double(local_AtA_[i+j*nrows]);
            }
            out.write_list_end();
        }
    }

private:
  double *local_AtA_;
};

class RowSum : SerialTSQR {
public:
    RowSum(TypedBytesInFile& in_, TypedBytesOutFile& out_,
	   size_t blocksize_, size_t rows_per_record_)
      : SerialTSQR(in_, out_, blocksize_, rows_per_record_) {}


  int read_key() {
    TypedBytesType type = in.next_type();
    if (type != TypedBytesInteger) {
      hadoop_message("invalid key, TypedBytes code: %d (skipping) \n",type);
      in.skip_next();
      return -1;
    }
    return in.read_int();
  }
    
  /** Handle the first input row.
   * The first row of the input is special, and so we handle
   * it differently.
   */
  virtual void first_row() {
    int row_index = read_key();
    std::vector<double> row;
    read_full_row(row);
    ncols = row.size();
    rows_.resize(ncols);
    used_.resize(ncols);
    assert(row_index < (int)ncols);
    for (int i = 0; i < (int)ncols; ++i) {
      rows_[i] = NULL;
      used_[i] = false;
    }
    hadoop_message("matrix size: %zi ncols, up to %i localrows\n", 
		   ncols, blocksize*ncols);
    add_row(row, row_index);
  }
    
  // read in a row and add it to the local matrix
  virtual void add_row(const std::vector<double>& row, int row_index) {
    assert(row.size() == ncols);
    if(used_[row_index]) {
      double *curr_row = rows_[row_index];
      double t0 = sf_time();
      lapack_daxpy((int)ncols, curr_row, (double *)&row[0]);
      double dt = sf_time() - t0;
      hadoop_counter("lapack time (millisecs)", (int)(dt*1000.));
    } else {
      double *new_row = (double *)malloc(ncols*sizeof(double));
      for (int i = 0; i < (int)ncols; ++i) {
	new_row[i] = row[i];
      }
      rows_[row_index] = new_row;
      used_[row_index] = true;
    }
  }

  virtual void reducer() {
    std::vector<double> row;
    first_row(); // handle the first row
    while (!feof(in.stream)) {
      int row_index = read_key();
      if (row_index == -1) continue;
      read_full_row(row);
      add_row(row, row_index);
    }
    hadoop_status("final output");
    output();
  }
  
  // compress the local factorization
  // do-nothing in this case (only need final output)
  void compress() {}
  
  // Output the row sums
  void output() {
    assert(rows_.size() == used_.size());
    int nrows = (int)rows_.size();
    for (int i = 0; i < nrows; ++i) {
      if (used_[i]) {
	out.write_int(i);
	out.write_list_start();
	double *curr_row = rows_[i];
	for (size_t j=0; j<ncols; ++j) {
	  out.write_double(curr_row[j]);
	}
	out.write_list_end();
	free(curr_row);
      }
    }
  }
  
private:
  std::vector<double*> rows_;
  std::vector<bool> used_;
};


void raw_mapper(TypedBytesInFile& in, TypedBytesOutFile& out, size_t blocksize,
                size_t rows_per_record, bool use_chol)
{
  if (!use_chol) {
    HouseTSQR map(in, out, blocksize, rows_per_record);
    map.mapper();
  } else {
    AtA map(in, out, blocksize, rows_per_record);
    map.mapper();    
  }
}

void raw_reducer(TypedBytesInFile& in, TypedBytesOutFile& out, size_t blocksize,
		 size_t rows_per_record, bool use_chol, int iteration)
{
  if (!use_chol) {
    raw_mapper(in, out, blocksize, rows_per_record, use_chol);
  } else {
    if (iteration == 0) {
      RowSum red(in, out, blocksize, rows_per_record);
      red.reducer(); 
    } else {
      Cholesky red(in, out, blocksize, rows_per_record);
      red.reducer(); 
    }
    return;    
  }
}

void usage() {
    fprintf(stderr, "usage: tsqr map|reduce [blocksize] [rowsperrecord] [cholesky]\n");
    exit(-1);
}

int main(int argc, char** argv) 
{  
    // initialize the random number generator
    unsigned long seed = sf_randseed();
    hadoop_message("seed = %u\n", seed);
    
    // create typed bytes files
    TypedBytesInFile in(stdin);
    TypedBytesOutFile out(stdout);
    
    if (argc < 2) {
        usage();
    }
    
    size_t blocksize = 3;
    if (argc > 2) {
        int bs = atoi(argv[2]);
        if (bs <= 0) {
            fprintf(stderr, 
              "Error: blocksize \'%s\' is not a positive integer\n",
              argv[2]);
        }
        blocksize = (size_t)bs;
    }

    size_t rows_per_record = 1;
    if (argc > 3) {
      int rpr = atoi(argv[3]);
      if (rpr <= 0) {
        fprintf(stderr, "Error: \'%s\' rows per record is not a positive integer\n", argv[3]);
      }
      rows_per_record = (size_t)rpr;
    }

    bool use_chol = false;
    if (argc > 4) {
      char *chol = argv[4];
      if (!strcmp(chol, "chol") || !strcmp(chol, "cholesky")) {
        use_chol = true;
      } else {
        //usage();
      }
    }

    int it = 0;
    if (argc > 5) {
      it = atoi(argv[5]);
    }

    char* operation = argv[1];
    if (strcmp(operation,"map")==0) {
      raw_mapper(in, out, blocksize, rows_per_record, false);
    } else if (strcmp(operation,"reduce") == 0) {
      raw_reducer(in, out, blocksize, rows_per_record, false, it);
    } else {
      usage();
    }
    return (0);
}
