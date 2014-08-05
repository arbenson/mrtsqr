"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
Direct TSQR algorithm (part 2).

Use the script run_dirtsqr.py to run Direct TSQR.
"""

import mrmc
import dumbo
import util
import os
import dirtsqr

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    compute_svd = gopts.getintkey('svd')
    mapper = mrmc.ID_MAPPER
    reducer = dirtsqr.DirTSQRRed2(compute_svd)
    job.additer(mapper=mapper, reducer=reducer,
                opts=[('numreducetasks', str(1))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-dirtsqr-2%s'%(matname,matext))
    
    gopts.getintkey('svd', 0)
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
