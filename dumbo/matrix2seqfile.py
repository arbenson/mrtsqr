"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
Convert a textual matrix file into a sequence file of typed bytes.

Usage:
     dumbo start matrix2seqfile.py -hadoop $HADOOP_INSTALL \
     -input [name of input file]
     -output [name of output file]

Example usage:
     dumbo start matrix2seqfile.py -hadoop $HADOOP_INSTALL \
           -input matrix.txt -output matrix.mseq

"""

import sys

def mapper(key, value):
    valarray = [float(v) for v in value.split()]
    if len(valarray) == 0:
        return
    yield key, valarray
    
if __name__ == '__main__':
    import dumbo
    import dumbo.lib

    dumbo.run(mapper, dumbo.lib.identityreducer)
