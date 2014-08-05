/**
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
*/

/**
 * @file sf_util.cc
 * Sparse matrix utility routines
 */

/** History
 * 2008-09-01: Initial coding
 */
/**
 * @file sf_util.cc
 * Sparse matrix utility routines
 */

/** History
 * 2008-09-01: Initial coding
 */


#include "sparfun_util.h"

#include <random>

#if defined(_WIN32) || defined(_WIN64)
# pragma warning(disable:4996)
# include <random>
# define tr1ns std::tr1
#elif defined __GNUC__
# define GCC_VERSION (__GNUC__ * 10000           \
                      + __GNUC_MINOR__ * 100     \
                      + __GNUC_PATCHLEVEL__)
# if GCC_VERSION < 40600
#  include <tr1/random>
#  define tr1ns std::tr1
# else
#  include <random>
#  define tr1ns std
# endif
#else
# include <random>
# define tr1ns std  
#endif  
    

#include <sys/types.h>
#include <sys/timeb.h>
double sf_time() {
#if defined(_WIN32) || defined(_WIN64)
  struct __timeb64 t; _ftime64(&t);
  return (t.time * 1.0 + t.millitm / 1000.0);
#else
  struct timeval t; gettimeofday(&t, 0);
  return (t.tv_sec * 1.0 + t.tv_usec / 1000000.0);
#endif
}

tr1ns::mt19937 sparfun_rand;

typedef tr1ns::mt19937 generator_t;
typedef tr1ns::uniform_real<double> distribution_t;
typedef tr1ns::variate_generator<generator_t, distribution_t> variate_t;
variate_t sparfun_rand_unif(sparfun_rand, distribution_t(0.0, 1.0));

void sf_srand(unsigned long seed) {
  sparfun_rand.seed(seed);
  sparfun_rand_unif = variate_t(sparfun_rand, distribution_t(0.0, 1.0));
}

// Return a seed based on the time.
unsigned long sf_timeseed(void) {
  unsigned long seed = (unsigned long) sf_time();
  sf_srand(seed);
  return seed;
}

// Return a seed based on random_device.
unsigned long sf_randseed(void) {
  tr1ns::random_device dev;
  unsigned long seed = (unsigned long) dev();
  sf_srand(seed);
  return seed;
}

double sf_rand(double min, double max) {
  tr1ns::uniform_real<double> dist(min,max);
  return dist(sparfun_rand_unif);
}

unsigned char sf_randbyte(void) {
  tr1ns::uniform_int<unsigned char> dist(0,255);
  return dist(sparfun_rand_unif);   
}

unsigned int sf_rand_uint(void) {
  tr1ns::uniform_int<unsigned int>
      dist(0, std::numeric_limits<unsigned int>::max());
  return dist(sparfun_rand_unif);
}

int sf_randint(int min, int max) {
  tr1ns::uniform_int<int> dist(min,max);
  return dist(sparfun_rand_unif);
}
