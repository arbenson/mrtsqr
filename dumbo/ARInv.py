#!/usr/bin/env dumbo

"""
ARInv.py
===========

Compute AR^{-1}
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

class ARInv(base.MatrixHandler):
    def __init__(self,blocksize=3,ncols=10,rpath='r.txt'):
        self.blocksize=blocksize
        self.nrows = 0
        self.data = []
        self.ncols = ncols
        self.row = None
        self.parseR(rpath)
        self.keys = []


    def parseR(self, rpath):
        f = open(rpath, 'r')
        data = []
        for line in f:
            row = line.strip(' ')
            row = row.split(',')
            row = row[1:]
            for i, entry in enumerate(row):
                row[i] = entry.strip(' ')
            if len(row) > 0:
                row[-1] = row[-1].strip('\n')
                row = [float(v) for v in row]
                data.append(row)                   
        R = numpy.mat(data)
        self.RPinv = numpy.linalg.pinv(R)

            
    def compress(self):        
        self.counters['AR^{-1} Computations'] += 1
        # compress the data
        
        # Compute AR^{-1} on the data accumulated so far
        if self.ncols is None:
            return
        
        t0 = time.time()
        A = numpy.mat(self.data)
        if A.size == 0:
            return

        ARInv = numpy.dot(A, self.RPinv)
        
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and add flushed update to local copy
        self.data = []
        for i, row in enumerate(ARInv.getA()):
            yield self.keys[i], struct.pack('d'*len(row), *row)

        # clear the keys
        self.keys = []
    
    def collect(self,key,value):
        if not len(value) == self.ncols:
            return

        self.keys.append(key)        
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def buffer_full(self):
        return len(self.data)>self.blocksize*self.ncols

    def __call__(self,data):
        deduced = False        
        # map job
        for key,value in data:
            if isinstance(value, str):
                if not deduced:
                    deduced = self.deduce_string_type(value)
                # handle conversion from string
                if self.unpacker is not None:
                    value = self.unpacker.unpack(value)
                else:
                    value = [float(p) for p in value.split()]
            self.collect(key,value)
            # if we accumulated enough rows, output some data
            if self.buffer_full():
                for key, val in self.compress():
                    yield key, val
                    
        # finally, output data
        for key, val in self.compress():
            yield key, val

    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    rpath = gopts.getstrkey('rpath2')

    mapper = ARInv(blocksize=blocksize,ncols=ncols,rpath=rpath)
    reducer = base.ID_REDUCER
    job.additer(mapper, reducer, [('numreducetasks',str(int(0)))])

def starter(prog):
    gopts.prog = prog

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"    
    
    rpath = prog.delopt('rpath')
    if not rpath:
        return "'rpath' not specified"
    prog.addopt('file',os.path.join(mypath,rpath))    
    gopts.getstrkey('rpath2','r.txt')


    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-arinv%s'%(matname,matext))    
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getintkey('ncols', -1)
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
