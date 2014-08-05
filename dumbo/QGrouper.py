"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
Q grouper class used by recursive Direct TSQR.

Use the script run_rec_dirtsqr.py to run recursive Direct TSQR.
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
    mapper = dirtsqr.QGrouperMap()
    reducer = dirtsqr.QGrouperReduce(ncols)
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
