#!/usr/bin/env bash
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

echo
echo
echo
echo New run
echo `date` >> experiment.log
echo

../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-50.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-100.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-500.mseq | tee -a experiment.log
../cxx/submit_colsums.sh tsqr-mr/test/mat-500g-1000.mseq | tee -a experiment.log

