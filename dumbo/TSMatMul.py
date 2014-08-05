"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
TSMatMul.py
===========

Compute A * B, where A is tall-and-skinny, and B is small.

Usage:
Usage:
     dumbo start ARInv.py -hadoop $HADOOP_INSTALL \
     -mat [name of matrix file] \
     -matpath [local path to small B] \
     -blocksize [optional: block size for compression]

Example usage:
     dumbo start TSMatMul.py -hadoop $HADOOP_INSTALL \
      -mat A_800M_10.bseq -matpath B_10_10.txt
"""

import mrmc
import dumbo
import util
import os

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    blocksize = gopts.getintkey('blocksize')
    matpath = gopts.getstrkey('matpath')

    mapper = mrmc.TSMatMul(blocksize=blocksize, matpath=matpath)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks',str(0))])

def starter(prog):
    gopts.prog = prog

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    matpath = prog.delopt('matpath')
    if not matpath:
        return "'matpath' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), matpath))

    gopts.getstrkey('matpath', matpath)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-matmul%s' % (matname, matext))    
    
    gopts.getintkey('blocksize', 50)
    gopts.getstrkey('reduce_schedule', '1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
