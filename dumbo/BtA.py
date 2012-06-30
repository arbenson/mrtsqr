#!/usr/bin/env dumbo

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
