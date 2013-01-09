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
    ncols = gopts.getintkey('ncols')
    reducetasks = gopts.getintkey('reducetasks')
    mapper = mrmc.ID_MAPPER
    reducer = dirtsqr.DirTSQRRed3(ncols)
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(reducetasks))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-dirtsqr-rec-3%s'%(matname,matext))

    rec_mat = prog.delopt('rec_mat')
    if not rec_mat:
        return "'rec_mat' not specified"
    prog.addopt('input', rec_mat)
    
    gopts.getintkey('ncols', 10)
    gopts.getintkey('reducetasks', 400)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
