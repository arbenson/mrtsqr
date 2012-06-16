#!/usr/bin/env dumbo

"""
Full TSQR algorithm for MapReduce (part 2)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import os
import full

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    compute_svd = gopts.getintkey('svd')
    mapper = mrmc.ID_MAPPER
    reducer = full.FullTSQRRed2(compute_svd)
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(1))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-full-tsqr-2%s'%(matname,matext))
    
    gopts.getintkey('svd', 0)
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
