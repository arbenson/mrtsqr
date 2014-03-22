"""
CholeskyQR.py
===========

Compute R from the QR factorization using Cholesky QR.
The factor R is computed by the Cholesky factorization of A^TA:

  A = QR --> A^TA = R^TR

Usage:
     dumbo start CholeskyQR.py -hadoop $HADOOP_INSTALL \
     -mat [name of matrix file] \
     -ncols [number of columns in the matrix] \
     -reduce_schedule [optional: number of reducers to use in each stage] \
     -output [optional: name of output file] \
     -matpath [optional: local path to small matrix for pre-multiplication] \
     -blocksize [optional: block size for compression]

The option 'matpath' can be used to provide a small matrix pre-multiplication
in one MapReduce step.  For example, if the the pre-multiplication file is M,
then this option computes the QR factorization of A * M without any additional
passes over the data.

Example usage:
     dumbo start CholeskyQR.py -hadoop $HADOOP_INSTALL \
     -mat A_800M_10.bseq -ncols 10 -reduce_schedule 40,1


Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
"""

import dumbo
import mrmc
import os
import sys
import util

# create the global options structure
gopts = util.GlobalOptions()
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    matpath = gopts.getstrkey('matpath')
    if matpath == '':
        matpath = None
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        nreducers = int(part)
        if i == 0:
            mapper = mrmc.AtA(blocksize=blocksize, premult_file=matpath)
            reducer = mrmc.ArraySumReducer
        else:
            mapper = mrmc.ID_MAPPER
            reducer = mrmc.Cholesky(ncols=ncols)
            nreducers = 1
        job.additer(mapper=mapper, reducer=reducer,
                    opts=[('numreducetasks', str(nreducers))])

def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getintkey('ncols', -1)    

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"

    matpath = prog.delopt('matpath')
    if matpath:
        prog.addopt('file', os.path.join(os.path.dirname(__file__), matpath))
        gopts.getstrkey('matpath', matpath)
    else:
        gopts.getstrkey('matpath', '')
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-chol-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
