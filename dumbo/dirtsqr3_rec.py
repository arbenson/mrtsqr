"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
Recursive Direct TSQR algorithm (part 3).

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
    prog.addopt('jobconf', 'mapred.reduce.max.attempts=30')
    prog.addopt('jobconf', 'mapred.max.tracker.failures=12')
    
    gopts.getintkey('ncols', 10)
    gopts.getintkey('reducetasks', 400)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
