"""
CholeskyQR.py
===========

Compute R from the QR factorization using Cholesky QR.
The factor R is computed by the Cholesky factorization of A^TA:

  A = QR --> A^TA = R^TR


Example usage:
dumbo start CholeskyQR.py -mat A_800M_10.bseq -ncols 10 -nummaptasks 30 \
-reduce_schedule 20,1 -hadoop icme-hadoop1


Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2013-2014
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
    mpath = gopts.getstrkey('mpath')
    if mpath == '': mpath = None
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        nreducers = int(part)
        if i == 0:
            mapper = mrmc.AtA(blocksize=blocksize, premult_file=mpath)
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

    mpath = prog.delopt('mpath')
    if mpath:
        prog.addopt('file', os.path.join(os.path.dirname(__file__), mpath))
        gopts.getstrkey('mpath', mpath)
    else:
        gopts.getstrkey('mpath', '')
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-chol-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
