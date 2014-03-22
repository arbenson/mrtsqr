"""
tssvd.py
===========

Compute the singular values of a tall-and-skinny matrix.

Usage:
     dumbo start tssvd.py -hadoop $HADOOP_INSTALL \
     -mat [name of matrix file] \
     -reduce_schedule [optional: number of reducers to use in each stage] \
     -output [optional: name of output file] \
     -blocksize [optional: block size for compression]


Example usage:
     dumbo start tssvd.py -hadoop $HADOOP_INSTALL \
     -mat A_800M_10.bseq -output A_singvals \
     -reduce_schedule 40,1

Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
"""

import mrmc
import dumbo
import util
import os

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        isfinal = (i == len(schedule) - 1)
        nreducers = int(part)
        mapper = mrmc.SerialTSQR(blocksize=blocksize, isreducer=False,
                                 isfinal=isfinal)
        reducer = mrmc.SerialTSQR(blocksize=blocksize, isreducer=True,
                                  isfinal=isfinal, svd=isfinal)
        job.additer(mapper=mapper,reducer=reducer,
                    opts = [('numreducetasks', str(nreducers))])    

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
        prog.addopt('output', '%s-svd%s'%(matname, matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
