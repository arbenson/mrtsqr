#!/usr/bin/env dumbo

"""
tsqr.py
===========

Driver code for tsqr.


Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import base
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
        if part.startswith('s'):
            base.add_splay_iteration(job, part)
        else:
            nreducers = int(part)
            if i == 0:
                mapper = mrmc.SerialTSQR(blocksize=blocksize, isreducer=False,
                                         isfinal=False)
                isfinal = False
            else:
                mapper = base.ID_MAPPER
                isfinal = True
            job.additer(mapper=mapper,
                        reducer=mrmc.SerialTSQR(blocksize=blocksize,
                                                isreducer=True,
                                                isfinal=isfinal),
                        opts = [('numreducetasks', str(nreducers))])    

def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output', '%s-qrr%s'%(matname, matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
