"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
Direct TSQR algorithm (part 3).

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
