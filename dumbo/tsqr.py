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
import struct

import numpy
import numpy.linalg

import util
import base

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class SerialTSQR(base.MatrixHandler):
    def __init__(self,blocksize=3,keytype='random',isreducer=False,isfinal=False):
        base.MatrixHandler.__init__(self)
        self.blocksize=blocksize
        if keytype=='random':
            self.keyfunc = lambda x: random.randint(0, 4000000000)
        elif keytype=='first':
            self.keyfunc = self._firstkey
        else:
            raise Error("Unkonwn keytype %s"%(keytype))
        self.first_key = None
        self.isreducer=isreducer
        self.nrows = 0
        self.data = []
        self.isfinal = isfinal
    
    def _firstkey(self, i):
        if isinstance(self.first_key, (list,tuple)):
            return (util.flatten(self.first_key),i)
        else:
            return (self.first_key,i)
    
    def QR(self):
        A = numpy.array(self.data)
        return numpy.linalg.qr(A,'r')
        
    def compress(self):
        """ Compute a QR factorization on the data accumulated so far. """
        if self.ncols is None:
            return
        if len(self.data) < self.ncols:
            return

        t0 = time.time()
        R = self.QR()
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(util.array2list(row))
                        
    def collect(self,key,value):
        if len(self.data) == 0:
            self.first_key = key
        
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            assert(len(value) == self.ncols)

        self.data.append(value)
        self.nrows += 1
        
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['QR Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        for i,row in enumerate(self.data):
            key = self.keyfunc(i)
            # If this is not the final output, we can use a TypedBytes String format
            if not self.isfinal:
                # If we already created the unpacker, then we can use it for efficiency
                if self.unpacker is not None:
                    yield key, self.unpacker.pack(*row)
                else:
                    yield key, struct.pack('d'*len(row), *row)
            else:
                yield key, row

    def __call__(self,data):
        deduced = False
        if self.isreducer == False:
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
                
        else:
            # determine the data format
            for key,values in data:
                for value in values:
                    if not deduced:
                        deduced = self.deduce_string_type(value)
                    if self.unpacker is not None:
                        val = self.unpacker.unpack(value)
                        self.collect(key,val)
                    else:
                        self.collect(key,value)
        # finally, output data
        for key,val in self.close():
            yield key, val
    
def runner(job):
    #niter = int(os.getenv('niter'))
    
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper = base.ID_MAPPER, reducer = base.ID_REDUCER,
                opts=[('numreducetasks',str(nreducers))])
            
        else:
            nreducers = int(part)
            if i==0:
                mapper = SerialTSQR(blocksize=blocksize,isreducer=False,isfinal=False)
                isfinal=False
            else:
                mapper = base.ID_MAPPER
                isfinal=True
            job.additer(mapper=mapper,
                        reducer=SerialTSQR(blocksize=blocksize,isreducer=True,isfinal=isfinal),
                        opts=[('numreducetasks',str(nreducers))])
    

def starter(prog):
    # set the global opts    
    gopts.prog = prog
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"
    
    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
