#!/usr/bin/env dumbo

"""
Austin R. Benson
David F. Gleich

copyright 2012

Full TSQR algorithm for MapReduce
"""

import sys
import os
import time
import random
import struct
import uuid

import numpy
import numpy.linalg

import util
import base

import dumbo
from dumbo import opt

# create the global options structure
gopts = util.GlobalOptions()

"""
FullTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""
@opt("getpath", "yes")
class FullTSQRMap1(base.MatrixHandler):
    def __init__(self):
        base.MatrixHandler.__init__(self)        
        self.nrows = 0
        self.keys = []
        self.data = []
        self.mapper_id = uuid.uuid1().hex
    
    def collect(self,key,value):
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            assert(len(value) == self.ncols)

        self.keys.append(key)
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows%50000

        # if no data was passed to this task, we just return
        if len(self.data) == 0:
            return

        A = numpy.array(self.data)
        QR = numpy.linalg.qr(A)        
        self.Q = QR[0].tolist()
        self.R = QR[1].tolist()

        for i, row in enumerate(self.R):
            yield ("R_%s" % str(self.mapper_id), self.mapper_id), row

        for i, row in enumerate(self.Q):
            key = self.keys[i]
            row.append(key)
            self.Q[i] = row

        yield ("Q_%s" % str(self.mapper_id), self.mapper_id), self.Q

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

        # finally, output data
        for key,val in self.close():
            yield key, val

def runner(job):
    mapper = FullTSQRMap1()
    reducer = base.ID_REDUCER
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(0))])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    # TODO: change default output
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))
    
    gopts.getstrkey('reduce_schedule','1')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
