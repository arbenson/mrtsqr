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
import dumbo.backends.common
from dumbo import opt
from dumbo.lib import MultiMapper, JoinReducer
from dumbo.decor import primary, secondary

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

        A = numpy.array(self.data)
        QR = numpy.linalg.qr(A)        
        self.Q = QR[0].tolist()
        self.R = QR[1].tolist()

        for i, row in enumerate(self.Q):
            key = self.keys[i]
            row.append(key)
            yield ("Q_%s" % str(self.mapper_id), self.mapper_id), row

        for i, row in enumerate(self.R):
            yield ("R_%s" % str(self.mapper_id), self.mapper_id), row

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
    print "running starter!"
    
    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath
    
    # set the global opts
    gopts.prog = prog

    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"

    prog.addopt('input',mat)

    prog.addopt('memlimit','2g')
    
    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')
        
    prog.addopt('file',os.path.join(mypath,'util.py'))

    matname,matext = os.path.splitext(mat)
    
    gopts.getstrkey('reduce_schedule','1')
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
