"""
Convert a textual matrix file into a sequence file of typed bytes

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
