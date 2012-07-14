#!/usr/bin/env dumbo

"""
BtA.py
===========

Driver code for B^T*A.

Example usage:
dumbo start BtA.py -hadoop icme-hadoop1 -matB B_matrix.mseq \
-matA A_matrix.mseq -output BTA_OUT -B_id B_matrix -blocksize 10 \
-reduce_schedule 10 -nummaptasks 40

B_id is a unique identifier for the path of the B matrix that does not
occur in the path to the A matrix.


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
    schedule = int(schedule)
    B_id = gopts.getstrkey('B_id')
    if B_id == '':
        print "'B_id' not specified"
        sys.exit(-1)

    job.additer(mapper=mrmc.BtAMapper(B_id=B_id),
                reducer=mrmc.BtAReducer(blocksize=blocksize),
                opts=[('numreducetasks', str(schedule))])
    job.additer(mapper='org.apache.hadoop.mapred.lib.IdentityMapper',
                reducer=mrmc.ArraySumReducer,
                opts=[('numreducetasks','1')])


def starter(prog):
    # set the global opts
    gopts.prog = prog

    matB = prog.delopt('matB')
    if not matB:
        return "'matB' not specified'"
    matA = prog.delopt('matA')
    if not matA:
        return "'matA' not specified'"

    gopts.getstrkey('B_id', '')

    mrmc.starter_helper(prog)
        
    prog.addopt('input', matB)
    prog.addopt('input', matA)

    matname, matext = os.path.splitext(matA)
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-BtA%s'%(matname,matext))
        
    gopts.save_params()
    
if __name__ == '__main__':
    import dumbo
    dumbo.main(runner, starter)
