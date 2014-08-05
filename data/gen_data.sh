#!/bin/bash
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

mydir=`dirname "$0"`
mydir=`cd "$bin"; pwd`
cd $mydir
cd ../dumbo

dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-500g-50.mseq \
  -nrows 1000000000 -ncols 50 \
  -maprows  1000000 -maxlocal 100 \
  -nstages 3 -overwrite yes


dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-500g-100.mseq \
  -nrows 500000000 -ncols 100 \
  -maprows  500000 -maxlocal 200 \
  -nstages 3 -overwrite yes
  
dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-500g-500.mseq \
  -nrows 100000000 -ncols 500 \
  -maprows  100000 -maxlocal 1000 \
  -nstages 3 -overwrite yes  

dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-500g-1000.mseq \
  -nrows 50000000 -ncols 1000 \
  -maprows  50000 -maxlocal 2000 \
  -nstages 3 -overwrite yes

dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-500g-5000.mseq \
  -nrows 10000000 -ncols 5000 \
  -maprows  10000 -maxlocal 5000 \
  -nstages 4 -overwrite yes

dumbo start ../dumbo/generate_test_problems.py \
  -output tsqr-mr/test/mat-5g-100-bigblock.mseq \
  -nrows  5000000 -ncols 100 \
  -maprows 500000 -maxlocal 200 \
  -nstages 2 -overwrite yes

