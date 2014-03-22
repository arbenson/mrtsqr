"""
ARInv.py
===========

Compute A * R^{-1}, where A is tall-and-skinny, and R is small

Usage:
     dumbo start ARInv.py -hadoop $HADOOP_INSTALL \
     -mat [name of matrix file] \
     -matpath [local path to small R] \
     -matpath2 [optional: extra small matrix] \
     -blocksize [optional: block size for compression]

When matpath2 is provided, the computation is

  (A * R^{-1}) * R_2^{-1}

This is used in, for example, the pseudo-IR code.

Example usage:
     dumbo start ARInv.py -hadoop $HADOOP_INSTALL \
     -mat A_800M_10.bseq -matpath R.txt

Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
"""

import mrmc
import dumbo
import util
import os

# create the global options structure
gopts = util.GlobalOptions()
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    matpath = gopts.getstrkey('matpath')
    matpath2 = gopts.getstrkey('matpath2', '')
    if matpath2 == '':
        matpath2 = None
    mapper = mrmc.ARInv(blocksize=blocksize,matpath=matpath, matpath2=matpath2)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])    

def starter(prog):
    gopts.prog = prog

    mat = mrmc.starter_helper(prog)
    if not mat: return "'mat' not specified"    
    
    matpath = prog.delopt('matpath')
    if not matpath:
        return "'matpath' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), matpath))    
    gopts.getstrkey('matpath', matpath)

    matpath2 = prog.delopt('matpath2')
    if matpath2:
        prog.addopt('file', os.path.join(os.path.dirname(__file__), matpath2))
        gopts.getstrkey('matpath2', matpath2)
    else:
        gopts.getstrkey('matpath2', '')

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-arinv%s' % (matname, matext))    
    
    gopts.getintkey('blocksize', 3)
    gopts.getstrkey('reduce_schedule', '1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
