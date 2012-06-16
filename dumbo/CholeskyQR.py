#!/usr/bin/env dumbo

"""
Cholesky.py
===========

Implement a Cholesky QR algorithm using dumbo and numpy.

Example usage:
dumbo start CholeskyQR.py -mat A_800M_10.bseq -ncols 10 -nummaptasks 30 \
-reduce_schedule 20,1 -hadoop icme-hadoop1


Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
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
    ncols = gopts.getintkey('ncols')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            mrmc.add_splay_iteration(job, part)
        else:
            nreducers = int(part)
            if i == 0:
                mapper = mrmc.AtA(blocksize=blocksize, isreducer=False,
                                  ncols=ncols)
                reducer = mrmc.AtA(blocksize=blocksize, isreducer=True,
                                   ncols=ncols)
            else:
                mapper = mrmc.ID_MAPPER
                reducer = mrmc.Cholesky(ncols=ncols)
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
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-chol-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
