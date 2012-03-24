#!/usr/bin/env dumbo

"""
Cholesky.py
===========

Implement a Cholesky QR algorithm using dumbo and numpy.

Assumes that matrix is stored as a typedbytes vector and that
the user knows how many columns are in the matrix.
"""

import sys
import os
import time
import random
import struct

import numpy
import numpy.linalg

import util
import base

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class Cholesky(dumbo.backends.common.MapRedBase):
    def __init__(self,ncols=10):
        self.ncols = ncols
        self.data = [numpy.zeros((1, ncols)) for x in range(ncols)]
    
    def close(self):
        L = numpy.linalg.cholesky(self.data)
        M = numpy.mat(L.T)
        for ind, row in enumerate(M.getA()):
            yield ind, row

    def __call__(self,data):
        for key,values in data:
            for value in values:
                self.data[key] += numpy.array(list(struct.unpack('d'*self.ncols, value)))
                
        for key,val in self.close():
            yield key, val

class AtA(base.MatrixHandler):
    def __init__(self,blocksize=3,isreducer=False,ncols=10):
        self.blocksize=blocksize
        self.isreducer=isreducer
        self.nrows = 0
        self.data = []
        self.A_curr = None
        self.row = None
    
    def compress(self):
        # Compute AtA on the data accumulated so far
        if self.ncols is None:
            return
        if len(self.data) < self.ncols:
            return
            
        t0 = time.time()
        A_mat = numpy.mat(self.data)
        A_flush = A_mat.T*A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and add flushed update to local copy
        self.data = []
        if self.A_curr == None:
            self.A_curr = A_flush
        else:
            self.A_curr = self.A_curr + A_flush

    
    def collect(self,key,value):
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            if not len(value) == self.ncols:
                return
        
        self.data.append(value)
        self.nrows += 1
        
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['AtA Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        if self.A_curr is not None:
            for ind, row in enumerate(self.A_curr.getA()):
                r = util.array2list(row)
                yield ind, struct.pack('d'*len(r),*r)

            
    def __call__(self,data):
        deduced = False
        if self.isreducer == False:
            # map job
            if isinstance(value, str):
                if not deduced:
                    deduced = self.deduce_string_type(value)
                # handle conversion from string
                if self.unpacker is not None:
                    value = self.unpacker.unpack(value)
                else:
                    value = [float(p) for p in value.split()]            
            for key,value in data:
                value = list(struct.unpack('d'*self.ncols, value))
                self.collect(key,value)

        else:
            for key,values in data:
                for value in values:
                    val = list(struct.unpack('d'*self.ncols, value))
                    if self.row == None:
                        self.row = numpy.array(val)
                    else:
                        self.row = self.row + numpy.array(val)                        
                yield key, struct.pack('d'*len(self.row),*self.row)

        # finally, output data
        if self.isreducer == False:
            for key,val in self.close():
                yield key, val
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    if ncols <= 0:
       sys.exit('ncols must be a positive integer')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper = base.ID_MAPPER, reducer = base.ID_REDUCER,
                opts=[('numreducetasks',str(nreducers))])
            job.additer(mapper, reducer, opts=[('numreducetasks',str(nreducers))])
            
        else:
            nreducers = int(part)
            if i==0:
                mapper = AtA(blocksize=blocksize,isreducer=False,ncols=ncols)
                reducer = AtA(blocksize=blocksize,isreducer=True,ncols=ncols)
            else:
                mapper = base.ID_MAPPER
                reducer = Cholesky(ncols=ncols)
            job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks',str(nreducers))])

def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getintkey('ncols', -1)    

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-chol-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
