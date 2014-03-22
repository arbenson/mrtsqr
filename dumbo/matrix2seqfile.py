"""
Convert a textual matrix file into a sequence file of typed bytes.

Usage:
     dumbo start matrix2seqfile.py -hadoop $HADOOP_INSTALL \
     -input [name of input file]
     -output [name of output file]

Example usage:
     dumbo start matrix2seqfile.py -hadoop $HADOOP_INSTALL \
           -input matrix.txt -output matrix.mseq

Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
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
