#!/bin/bash -l
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

export LDPATH="/usr/lib64/atlas:$LDPATH"
export LDPATH="/usr/lib64/:$LDPATH"
export PATH="/usr/lib64/:$PATH"
chmod a+rwx tsqr
./tsqr $@

