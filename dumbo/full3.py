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
    q2path = gopts.getstrkey('q2path')
    ncols = gopts.getintkey('ncols')
    mapper = full.FullTSQRMap3(q2path,ncols)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-full-tsqr-3%s'%(matname,matext))
    
    gopts.getintkey('ncols', 10)

    q2path = prog.delopt('q2path')
    if not q2path:
        return "'q2path' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), q2path))

    gopts.getstrkey('q2path', q2path)
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)

