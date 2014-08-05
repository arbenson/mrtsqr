"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
AtA.py
===========

Compute A^T * A for a matrix A.

Usage:
     dumbo start AtA.py -hadoop $HADOOP_INSTALL \
     -mat [name of matrix file] \
     -reduce_schedule [optional: number of reducers to use in each stage] \
     -output [optional: name of output file] \
     -blocksize [optional: block size for compression]

Example usage:
     dumbo start AtA.py -hadoop $HADOOP_INSTALL \
     -mat A_800M_10.bseq -output AtA_10x10.bseq \
     -blocksize 5 -reduce_schedule 40,1
"""

import os
import util
import sys
import dumbo
import time
import numpy
import mrmc

gopts = util.GlobalOptions()

def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        nreducers = int(part)
        if i == 0:
            mapper = mrmc.AtA(blocksize=blocksize)
            reducer = mrmc.ArraySumReducer
        else:
            mapper = mrmc.ID_MAPPER
            reducer = mrmc.ArraySumReducer()
            nreducers = 1
        job.additer(mapper=mapper, reducer=reducer,
                    opts=[('numreducetasks', str(nreducers))])


def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-ata%s'%(matname,matext))

    gopts.save_params()
    
if __name__ == '__main__':
    import dumbo
    dumbo.main(runner, starter)
