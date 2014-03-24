"""
GenBigExample.py
===========

This generates 1000 rows of a matrix with 20 columns for each key-value pair
in the input.

Example usage:
     dumbo start GenBigExample.py -mat Simple_10k.txt \
     -output A_10M_20.bseq -hadoop $HADOOP_INSTALL

Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
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

def Map(key, value):
	# For each key-value pair, emit a random 10,000 x 20 matrix.
	A = numpy.random.randn(1000, 20)
	for row in A:
		key = [numpy.random.randint(0, 1000000) for x in xrange(3)]
		yield key, struct.pack('d' * len(row), *row)

def runner(job):
    options=[('numreducetasks', '0'), ('nummaptasks', '20')]
    job.additer(mapper=Map, reducer=mrmc.ID_REDUCER,
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

    prog.addopt('file',os.path.join(mypath,'util.py'))
    prog.addopt('file',os.path.join(mypath,'mrmc.py'))

    prog.addopt('input', mat)
    matname,matext = os.path.splitext(mat)

    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-randn%s'%(matname,'.bseq'))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
