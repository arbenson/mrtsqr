#!/usr/bin/env dumbo

"""
AtA.py
===========

Driver code for A^T*A.

Example usage:
dumbo start AtA.py -mat A_matrix -reduce_schedule 1 -hadoop icme-hadoop1

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
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
        if part.startswith('s'):
            mrmc.add_splay_iteration(job, part)
        else:
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
