#!/bin/bash -l
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

# This is the script used to run the C++ script on NERSC machines
# usage: ./mkl_and_tsqr map|reduce [blocksize]
module load mkl && chmod a+rwx tsqr
./tsqr $@

