"""
Direct TSQR algorithm (part 1).

Use the script run_dirtsqr.py to run Direct TSQR.

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012-2014
"""

import mrmc
import dumbo
import util
import os
import dirtsqr

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    mapper = dirtsqr.DirTSQRMap1()
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
        prog.addopt('output','%s-dirtsqr1%s'%(matname,matext))
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
