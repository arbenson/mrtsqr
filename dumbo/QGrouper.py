#!/usr/bin/env dumbo

"""
R labeller for recursive Direct TSQR

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
    mapper = dirtsqr.QGrouper()
    reducer = dirtsqr.QGrouper2(ncols)
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(100))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    gopts.getintkey('ncols', 10)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qgrouped%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
