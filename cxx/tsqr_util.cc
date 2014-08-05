/**
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
*/

#include "tsqr_util.h"

#include <algorithm>
#include <vector>

#include "sparfun_util.h"
#include "typedbytes.h"

// Write a message to stderr
void hadoop_message(const char* format, ...) {
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end (args);
}

// Write a status message to hadoop.
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

// Write an error message to hadoop and then exit
void hadoop_error(const char* format, ...) {
  va_list args;
  va_start (args, format);  
  fprintf(stderr, "error: ");
  vfprintf(stderr, format, args);
  va_end(args);
  exit(-1);
}

void hadoop_counter(const char* name, int val) {
  fprintf(stderr, "reporter:counter:Program,%s,%i\n", name, val);
}

// Copy A (row-major) to B (col-major)
void row_to_col_major(double *A, double *B, size_t num_rows, size_t num_cols) {
  for (size_t i = 0; i < num_rows; ++i)
    for (size_t j = 0; j < num_cols; ++j)
      B[i + j * num_rows] = A[i * num_cols + j];
}

// Copy A (col-major) to B (row-major)
void col_to_row_major(double *A, double *B, size_t num_rows, size_t num_cols) {
  for (size_t i = 0; i < num_rows; ++i)
    for (size_t j = 0; j < num_cols; ++j)
      B[i * num_cols + j] = A[i + j * num_rows];
}

extern "C" {
  void dgeqrf_(int *m, int *n, double *a, int *lda, double *tau,
	       double *work, int *lwork, int *info);
  void dgeqr_(int *m, int *n, double *a, int *lda, double *tau,
	      double *work, int *lwork, int *info);
  void dorgqr_(int *m, int *n, int *k, double *a, int *lda, double *tau,
	       double *work, int *lwork, int *info);
  void dsyrk_(char *uplo, char *trans, int *m, int *k, double *alpha,
              double *A, int *lda, double *beta, double *C, int *ldc);
  void daxpy_(int *n, double *alpha, double *x, int *incx, double *y, int *incy);
  void dpotrf_(char *uplo, int *n, double *a, int *lda, int *info);
  void dgemm_(char *transa, char *transb, int *m, int *n, int *k, double *alpha,
              double *A, int *lda, double *B, int *ldb, double *beta, double *C,
              int *ldc);
}

/** Run a LAPACK daxpy
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 */
bool lapack_daxpy(size_t size, double *x, double *y) {
  int incx = 1;
  int incy = 1;
  int n = (int) size;
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
  if (info != 0) {
    fprintf(stderr, "matrix is not positive definite!, info is: %zi\n", info);
    exit(-1);
  }
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

bool _lapack_qr(double *A, size_t nrows, size_t ncols, size_t urows,
                std::vector<double>& tau) {
  int info = -1;
  int n = ncols;
  int m = urows;
  int stride = nrows;

  // do a workspace query
  double worksize;
  int lworkq = -1;  
  dgeqrf_(&m, &n, A, &stride, &tau[0], &worksize, &lworkq, &info);
  if (info != 0) {
    return false;
  }

  int lwork = (int) worksize;
  std::vector<double> work(lwork);
  dgeqrf_(&m, &n, A, &stride, &tau[0], &work[0], &lwork, &info);
  if (info != 0) {
    return false;
  }
  return true;
}

// zero out the lower triangle of A
void zero_out_lower_triangle(double *A, size_t rsize, size_t nrows) {
  for (size_t j = 0; j < rsize; ++j) {
    for (size_t i = j + 1; i < rsize; ++i) {
      A[i + j * nrows] = 0.;
    }
  }  
}


/*
 * Run a LAPACK qr with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_qr(double* A, size_t nrows, size_t ncols, size_t urows) {
  // allocate space for tau's
  int minsize = std::min(urows, ncols);
  std::vector<double> tau(minsize);
  if (!_lapack_qr(A, nrows, ncols, urows, tau)) {
    return false;
  }
  
  zero_out_lower_triangle(A, (size_t) minsize, nrows);
  return true;
}

/*
 * Run a LAPACK qr with explicit Q and R storage.
 * @param A is the matrix on which to perform QR
 * @param R is storage for the R matrix (Q is stored in A)
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_full_qr(double *A, double *R, size_t nrows, size_t ncols, size_t urows) {
  size_t minsize = std::min(urows, ncols);
  std::vector<double> tau(minsize);
  if (!_lapack_qr(A, nrows, ncols, urows, tau)) {
    return false;
  }

  for (size_t i = 0; i < minsize; ++i) {
    for (size_t j = 0; j < minsize; ++j) {
      if (i < j) {
        R[i + j * minsize] = 0;
      } else {
	R[i + j * minsize] = A[j + i * nrows];
      }
    }
  }

  int info = -1;
  int m = urows;
  int n = ncols;
  int k = ncols;
  int stride = nrows;

  // do a workspace query
  double worksize;
  int lworkq = -1;  
  dorgqr_(&m, &n, &k, A, &stride, &tau[0], &worksize, &lworkq, &info);
  if (info != 0) {
    return false;
  }

  int lwork = (int) worksize;
  std::vector<double> work(lwork);
  dorgqr_(&m, &n, &k, A, &stride, &tau[0], &work[0], &lwork, &info);
  if (info != 0) {
    return false;
  }

  return true;
}

bool lapack_tsmatmul(double *A, size_t nrows_A, size_t ncols_A,
		     double *B, size_t ncols_B, double *C) {
  hadoop_message("TSMATMUL\n");
  char transa = 'n';
  char transb = 'n';
  int m = (int) nrows_A;
  int n = (int) ncols_B;
  int k = (int) ncols_A;
  double alpha = 1;
  int lda = m;
  int ldb = k;
  double beta = 0;
  int ldc = m;
  
  // Store result in A, since we are assuming B is square
  dgemm_(&transa, &transb, &m, &n, &k, &alpha, A,
         &lda, B, &ldb, &beta, C, &ldc);

  hadoop_message("TSMATMUL success!\n");
  return true;
}
