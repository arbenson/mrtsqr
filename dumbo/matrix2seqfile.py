#!/usr/bin/env dumbo

"""
Convert a textual matrix file into a sequence file of typed bytes
"""

import sys

use_tb_str = False

"""
Map lines of a matrix to a sequence file:
  Key=<lineno>, Value=[row_i]
"""
def mapper(key,value):
    valarray = [float(v) for v in value.split()]
    if len(valarray) == 0:
        return
    # for typed bytes string output, set the following to True
    # TODO(arbenson): make this cleaner
    if use_tb_str:
        yield key, struct.pack('d'*len(valarray), *valarray)
    else:
        yield key, valarray
    
class Converter:
    def __init__(self,opts):
        pass
    def __call__(self,data):
        item = 0
        for key,value in data:
            for entry in value:
                print "%18.16e"%(entry), 
            print
            item += 1
    
if __name__ == '__main__':
    import dumbo
    import dumbo.lib
    if 'use_tb_str' in sys.argv:
        use_tb_str = True

    dumbo.run(mapper,dumbo.lib.identityreducer)

    
