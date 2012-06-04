#!/usr/bin/env dumbo

"""
TSMatMul.py
===========

Compute A*B, where A is tall-and-skinny, and B is small.
"""

import sys
import os
import time
import struct

import numpy
import numpy.linalg

import util
import base

import dumbo

# create the global options structure
gopts = util.GlobalOptions()

class TSMatMul(base.MatrixHandler):
    def __init__(self,blocksize=3,mpath='m.txt'):
        base.MatrixHandler.__init__(self)        
        self.blocksize=blocksize
        self.row = None
        self.data = []
        self.keys = []
        self.parseM(mpath)

    def parseM(self, mpath):
        f = open(mpath, 'r')
        data = []
        for line in f:
            if len(line) > 5:
                ind2 = line.rfind(')')
                line = line[ind2+3:]
                line = line.lstrip('[').rstrip().rstrip(']')
                try:
                    line2 = line.split(',')
                    line2 = [float(v) for v in line2]
                except:
                    line2 = line.split()
                    line2 = [float(v) for v in line2]
                data.append(line2)
        f.close()
        self.small = numpy.mat(data)

    def compress(self):        
        # Compute the matmul on the data accumulated so far
        if self.ncols is None or len(self.data) == 0:
            return

        self.counters['MatMul compression'] += 1

        t0 = time.time()
        A = numpy.mat(self.data)
        out_mat = A*self.small
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and add flushed update to local copy
        self.data = []
        for i, row in enumerate(out_mat.getA()):
            yield self.keys[i], row

        # clear the keys
        self.keys = []
    
    def collect(self,key,value):
        if self.ncols == None:
            self.ncols = len(value)
        
        if not len(value) == self.ncols:
            return

        self.keys.append(key)
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def buffer_full(self):
        return len(self.data) >= self.blocksize*self.ncols

    def __call__(self,data):
        for key,value in data:
            self.collect_data_instance(key, value)

            # if we accumulated enough rows, output some data
            if self.buffer_full():
                for key, val in self.compress():
                    yield key, val
                    
        # output data the end of the data
        for key, val in self.compress():
            yield key, val

def runner(job):
    blocksize = gopts.getintkey('blocksize')
    mpath = gopts.getstrkey('mpath')

    mapper = TSMatMul(blocksize=blocksize,mpath=mpath)
    reducer = base.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])    

def starter(prog):
    gopts.prog = prog

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"    
    
    mpath = prog.delopt('mpath')
    if not mpath:
        return "'mpath' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), mpath))    
    gopts.getstrkey('mpath', mpath)

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-arinv%s'%(matname,matext))    
    
    gopts.getintkey('blocksize',50)
    gopts.getstrkey('reduce_schedule','1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
