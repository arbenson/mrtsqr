#!/usr/bin/env dumbo

"""
ARInv.py
===========

Compute AR^{-1}
"""

import mrmc
import dumbo
import util
import os

# create the global options structure
gopts = util.GlobalOptions()
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    rpath = gopts.getstrkey('rpath')

    mapper = ARInv(blocksize=blocksize,rpath=rpath)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])    

def starter(prog):
    gopts.prog = prog

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"    
    
    rpath = prog.delopt('rpath')
    if not rpath:
        return "'rpath' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), rpath))    
    gopts.getstrkey('rpath', rpath)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-arinv%s'%(matname,matext))    
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
