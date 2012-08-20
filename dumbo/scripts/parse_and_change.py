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

class Map:
  def __init__(self, scale):
    self.scale = scale
    self.ncols = 10

  def __call__(self, key, value):
    if key != 0:
      return
    Q, R = numpy.linalg.qr(numpy.random.randn(10000, 10))
    s = [self.scale] + [1]*9
    A = numpy.mat(Q) * numpy.mat(R*s)
    Q2, R2 = numpy.linalg.qr(numpy.random.randn(10, 10))
    A = A * numpy.mat(Q2)
    for row in A.getA():
      key = [numpy.random.randint(0, 1000000) for x in xrange(3)]
      yield key, row

def runner(job):
    scale = gopts.getintkey('scale')
    options=[('numreducetasks', '0'), ('nummaptasks', '20')]
    job.additer(mapper=Map(scale=scale), reducer=mrmc.ID_REDUCER,
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

    gopts.getintkey('scale', 20)

    prog.addopt('memlimit','2g')

    prog.addopt('file',os.path.join(mypath,'util.py'))
    prog.addopt('file',os.path.join(mypath,'mrmc.py'))

    prog.addopt('input', mat)

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
