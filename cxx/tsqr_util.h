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
bool lapack_syrk(double* A, double* C, size_t nrows, size_t ncols,
                 size_t urows) {
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
bool lapack_qr(double* A, size_t nrows, size_t ncols, size_t urows) {
  int info = -1;
  int n = ncols;
  int m = urows;
  int stride = nrows;
  int minsize = std::min(urows, ncols);

  // allocate space for tau's
  std::vector<double> tau(minsize);
    
  // do a workspace query
  double worksize;
  int lworkq = -1;
    
  dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
  if (info != 0) {
    return false;
  }
  int lwork = (int) worksize;
  std::vector<double> work(lwork);
  dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
  if (info != 0) {
    return false;
  }
  // zero out the lower triangle of A
  size_t rsize = (size_t)minsize;
  for (size_t j = 0; j < rsize; ++j) {
    for (size_t i = j + 1; i < rsize; ++i) {
      A[i + j * nrows] = 0.;
    }
  }
  return true;
}

static void read_full_row(TypedBytesInFile& in, std::vector<double>& row) {
  TypedBytesType code = in.next_type();
  row.clear();
  if (code == TypedBytesVector) {
    hadoop_message("code == TypedBytesVector\n");
    typedbytes_length len = in.read_typedbytes_sequence_length();
    row.reserve((size_t)len);
    for (size_t i = 0; i < (size_t)len; ++i) {
      TypedBytesType nexttype = in.next_type();
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
    TypedBytesType nexttype = in.next_type();
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
      nexttype = in.next_type();
    }
  } else if (code == TypedBytesString) {
    hadoop_message("code == TypedBytesString\n");
    typedbytes_length len = in.read_string_length();
    row.resize(len / 8);
    in.read_string_data((unsigned char *)&row[0], (size_t)len);
  } else {
    hadoop_message("row is not a list, vector, or string\n");
    hadoop_message("code is: %d\n", code);
    fprintf(stderr,
	    "error: row %zi is a not a list, vector, or string\n", totalrows);
    exit(-1);
  }
}

static bool read_key_val_pair(TypedBytesInFile& in,
                              typedbytes_opaque& key,
                              std::vector<double>& value) {
  if (in.read_opaque(key) == false) {
    return false;
  }
  read_full_row(value); 
  return true;
}
