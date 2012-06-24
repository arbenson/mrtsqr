#!/usr/bin/env dumbo

"""
Cholesky.py
===========

Implement a Cholesky QR algorithm using dumbo and numpy.

The -name_id option is a unique identifer in the B matrix file name that
does not occur in the A matrix file name.

Example usage:
dumbo start BtA.py -A A_800M_10.bseq -B B_800M_10.bseq -name_id B -ncols 10 \
-output BtA_10_10.mseq -reduce_schedule 20,1 -hadoop icme-hadoop1


Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import os
from dumbo import opt

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    name_id = gopts.getstrkey('name_id')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        nreducers = int(part)
        if i == 0:
            mapper = mrmc.FileAppender()
            reducer = mrmc.BtA(name_id=name_id)
        else:
            mapper = mrmc.ID_MAPPER
            reducer = mrmc.RowSum(ncols=ncols)

        job.additer(mapper=mapper, reducer=reducer,
                    opts=[('numreducetasks', str(nreducers))])

def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','20,1')
    gopts.getintkey('ncols', -1)
    gopts.getstrkey('name_id', 'B')

    mrmc.starter_helper(prog)

    path = None
    for mat in ['A', 'B']:
        path = prog.delopt(mat)
        if not path:
            return '%s matrix not provided' % mat
        prog.addopt('input', path)
    
    matname, matext = os.path.splitext(path)
    output = prog.getopt('output')
    # TODO(arbenson): better default output name
    if not output:
        prog.addopt('output','%s-bta%s'%(matname, matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
