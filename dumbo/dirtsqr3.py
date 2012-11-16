#!/usr/bin/env dumbo

"""
Direct TSQR algorithm for MapReduce (part 3)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import os
import dirtsqr

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    q2path = gopts.getstrkey('q2path')
    upath = gopts.getstrkey('upath')
    if upath == '':
      upath = None
    ncols = gopts.getintkey('ncols')
    mapper = dirtsqr.DirTSQRMap3(ncols, q2path, upath)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks', '0')])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-dirtsqr-3%s'%(matname,matext))
    
    gopts.getintkey('ncols', 10)

    q2path = prog.delopt('q2path')
    if not q2path:
        return "'q2path' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), q2path))
    gopts.getstrkey('q2path', q2path)

    upath = prog.delopt('upath')
    if upath:
      prog.addopt('file', os.path.join(os.path.dirname(__file__), upath))
    else:
      upath = ''
    gopts.getstrkey('upath', upath)
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
