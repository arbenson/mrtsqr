#!/usr/bin/env dumbo

"""
MapReduce matrix computations.  This contains the basic building blocks.

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
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

"""
Serial TSQR
"""
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
        # Compute a QR factorization on the data accumulated so far.
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
        if not self.isreducer:
            self.collect_data(data)
        else:
            for key,values in data:
                self.collect_data(values, key)

        # finally, output data
        for key,val in self.close():
            yield key, val
