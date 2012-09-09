#ifndef MRTSQR_CXX_TSQR_UTIL_H_
#define MRTSQR_CXX_TSQR_UTIL_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string>

#include <vector>

// Write a message to stderr
void hadoop_message(const char* format, ...);

// Write a status message to hadoop.
void hadoop_status(const char* format, ...);

// Write an error message to hadoop and then exit
void hadoop_error(const char* format, ...);

void hadoop_counter(const char* name, int val);

// Copy A (row-major) to B (col-major)
void row_to_col_major(double *A, double *B, size_t num_rows, size_t num_cols);

// Copy A (col-major) to B (row-major)
void col_to_row_major(double *A, double *B, size_t num_rows, size_t num_cols);

/** Run a LAPACK daxpy
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 */
bool lapack_daxpy(int size, double *x, double *y);

/** Run a LAPACK Cholesky
 * @param A the matrix in lower triangular form
 * @param ncols the number of columns of A allocated
 */
bool lapack_chol(double *A, int ncols);

/** Run a LAPACK syrk with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_syrk(double* A, double* C, size_t nrows, size_t ncols,
                 size_t urows);

bool _lapack_qr(double *A, size_t nrows, size_t ncols, size_t urows,
                std::vector<double>& tau);

// zero out the lower triangle of A
void zero_out_lower_triangle(double *A, size_t rsize, size_t nrows);

/*
 * Run a LAPACK qr with local memory allocation.
 * @param nrows the number of rows of A allocated
 * @param ncols the number of columns of A allocated
 * @param urows the number of rows of A used.
 * In LAPACK parlance, nrows is the stride, and urows is
 * the size
 */
bool lapack_qr(double* A, size_t nrows, size_t ncols, size_t urows);

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
bool lapack_full_qr(double *A, double *R, size_t nrows, size_t ncols,
                    size_t urows);

bool lapack_tsmatmul(double *A, size_t nrows_A, size_t ncols_A,
		     double *B, size_t ncols_B, double *C);

#endif  // MRTSQR_CXX_TSQR_UTIL_H_
