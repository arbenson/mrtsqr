#!/usr/bin/env dumbo

"""
TSMatMul.py
===========

Compute A*B, where A is tall-and-skinny, and B is small.

Example usage:
dumbo start TSMatMul.py -hadoop icme-hadoop1 -mat A_800M_10.bseq \
-mpath B_10_10.txt

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
    mpath = gopts.getstrkey('mpath')
    mpath2 = gopts.getstrkey('mpath2')

    mapper = mrmc.TSMatMul(blocksize=blocksize, mpath=mpath, mpath2=mpath2)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks',str(0))])

def starter(prog):
    gopts.prog = prog

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    mpath = prog.delopt('mpath')
    if not mpath:
        return "'mpath' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), mpath))
    gopts.getstrkey('mpath', mpath)

    mpath2 = prog.delopt('mpath2')
    if not mpath2:
        return "'mpath2' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), mpath2))
    gopts.getstrkey('mpath2', mpath2)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-matmul%s' % (matname, matext))    
    
    gopts.getintkey('blocksize', 50)
    gopts.getstrkey('reduce_schedule', '1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
