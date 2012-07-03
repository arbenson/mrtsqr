#!/usr/bin/env dumbo

"""
tsqr.py
===========

Implement a tsqr algorithm using dumbo and numpy
"""

import sys
import os
import time
import random

import numpy
import struct

import util
import mrmc

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

def mapper(key,value):
    #x = key[0]
    #y = key[1]
    #z = key[2]
    #c = key[3]
    #k = (c-1)*(128*255*256) + (z-1)*(255*256) + (y-1)*(256) + (x-1)
    #k = key
    #value = value[0:2] 
    #v = value.split()
    #k = int(v[0])
    #v = v[1:]
    #value = [float(e) for e in v]
    use = random.randint(0, 4000000000)
    if not use % 100:
        yield key, value

def runner(job):
    options=[('numreducetasks', '0'), ('nummaptasks', '20')]
    job.additer(mapper=mapper, reducer=mrmc.ID_REDUCER,
                opts=options)

def starter(prog):
    print "running starter!"

    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath

    # set the global opts
    gopts.prog = prog


    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"

    prog.addopt('memlimit','2g')

    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')

    prog.addopt('file',os.path.join(mypath,'util.py'))
    prog.addopt('file',os.path.join(mypath,'mrmc.py'))

    numreps = prog.delopt('replication')
    if not numreps:
        numreps = 1
    for i in range(int(numreps)):
        prog.addopt('input',mat)

    #prog.addopt('input',mat)
    matname,matext = os.path.splitext(mat)

    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-randn%s'%(matname,'.bseq'))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
