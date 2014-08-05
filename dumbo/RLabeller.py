"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
R labeller for recursive Direct TSQR.

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
    mapper = dirtsqr.RLabeller()
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', '0')])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-rlabelled%s'%(matname,matext))
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
