#!/usr/bin/env python

import sys
import os
import time
import random

import numpy
import numpy.linalg

import util
import uuid

import dumbo
import dumbo.backends.common

from dumbo.lib import MultiMapper, JoinReducer
from dumbo.decor import primary, secondary

# create the global options structure
gopts = util.GlobalOptions()

"""
-------------------
Austin R. Benson
David F. Gleich

copyright 2012
-------------------
Full TSQR algorithm for MapReduce
-------------------

This algorithm computes the QR decomposition for a matrix.


Phase 1:
map: SimpleSerialTSQR
reduce: identity

Phase 2:
map:
reduce: FullTSQRRed2

Phase 3:
map: identity
reduce: FullTSQRRed3
"""

"""
TODO:
-Get multiple outputs working (need the feathers jar)
-Change FullTSQRRed3 from map task to reduce task
-Test, test, test
-Benchmark against AR^{-1} + IR
"""

"""
FullTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""

#@opt("getpath", "yes")
class FullTSQRMap1(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.nrows = 0
        self.keys = []
        self.data = []
        self.ncols = None
        self.mapper_id = uuid.uuid1().hex
        self.Q = None
        self.R = None
    
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
        A = numpy.array(self.data)
        QR = numpy.linalg.qr(A)        
        self.Q = QR[0].tolist()
        self.R = QR[1].tolist()
        
        self.counters['rows processed'] += self.nrows%50000


    def __call__(self, data):
        for key,value in data:
            if isinstance(value, str):
                # handle conversion from string
                value = [float(p) for p in value.split()]
            self.collect(key,value)
                
        self.close()
        for i, row in enumerate(self.Q):
            key = self.keys[i]
            row = row.append(key)
            print >>sys.stderr, "about to yield (Q)"
            yield self.mapper_id, row
            #yield ("Q_%d" % self.mapper_id, self.mapper_id), row
        for i, row in enumerate(self.R):
            print >>sys.stderr, "about to yield (R)"            
            yield self.mapper_id, row            
            #yield ("R_%d" % self.mapper_id, self.mapper_id), row        
        print >>sys.stderr, "done"

"""
FullTSQRRed2
------------

Takes all of the intermediate Rs

Computes [R_1, ..., R_n] = Q2R_{final}

Output:
1. R_final: R in A = QR with key-value pairs <i, row>
2. Q2: <mapper_id, row>

where Q2 is a list of key value pairs.

Each key corresponds to a mapperid from stage 1 and that keys value is the
Q2 matrix corresponding to that mapper_id
"""
#@opt("getpath", "yes")
class FullTSQRRed2(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.R_data = {}
        self.key_order = []
        self.Q2 = None

    def add_R(self, key, value):
        if key not in self.R_data:
            self.R_data[key] = []
        row = [float(val) for val in value]
        self.R_data[key].append(row)

    def close_R(self):
        data = []
        for key in self.R_data:
            data += self.R_data[key]
            self.key_order.append(key)
        Q, R = self.QR(data)
        self.Q2 = Q
        self.R_final = R
        for i, row in enumerate(R):
            yield ("R_final", i), row

    def close_Q(self):
        num_rows = len(self.Q2)
        rows_to_read = num_rows / len(self.key_order)

        ind = 0
        key_ind = 0
        key = self.key_order[key_ind]
        local_Q = []
        for row in self.Q2:
            local_Q.append(row)
            ind += 1
            if (ind == rows_to_read):
                yield ("Q2", self.key_order[key_ind]), local_Q
                key_ind += 1                
                local_Q = []
                ind = 0


    def __call__(self,data):
        for key,values in data:
                for value in values:
                    self.add_R(key, value)

        for key, val in self.close_R():
            yield key, val
        for key, val in self.close_Q():
            yield key, val


"""
FullTSQRRed3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class FullTSQRRed3(dumbo.backends.common.MapRedBase):
    def __init__(self):
        # TODO implement this
        self.parse_input_Q2()
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}

    # key1: unique mapper_id
    # key2: row identifier
    # value: row of Q1
    def collect(self, key1, key2, value):
        if key1 not in self.Q1_data:
            self.Q1_data[key1] = []
            assert(key1 not in self.row_keys)
            self.row_keys[key1] = []

        row = [float(val) for val in value]
        self.Q1_data[key1].append(row)
        self.row_keys[key1].append(key2)

    def close(self):
        for key in self.Q1_data:
            assert(key in self.row_keys)
            assert(key in self.Q2_data)
            Q1 = numpy.mat(self.Q1_data[key])
            Q2 = numpy.mat(self.Q2_data[key])
            Q_out = Q1*Q2
            for i, row in enumerate(Q_out.getA()):
                yield self.row_keys[key][i], row

    def __call__(self,data):
        for key1, values in data:
            for value in values:
                key2 = value[-1]
                val = val[0:-1]
                self.collect(key1, key2, value)            

        for key, val in self.close():
            yield key, val



def runner(job):    
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    print "schedule is: " + str(schedule)
    for i,part in enumerate(schedule):
        if i == 0:
            nreducers = int(part[1:])            
            map = FullTSQRMap1()
            red = "org.apache.hadoop.mapred.lib.IdentityReducer"
            job.additer(mapper=map, reducer=red, opts=[('numreducetasks',str(nreducers))])
            
        """
        elif i == 1:
            nreducers = 1
            map = "org.apache.hadoop.mapred.lib.IdentityMapper"
            red = FullTSQRRed2()
        elif i == 2:
            nreducers = int(part[1:])
            map = "org.apache.hadoop.mapred.lib.IdentityMapper"
            red = FullTSQRRed3()
        """

    

def starter(prog):
    print "running starter!"
    
    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath
    
    # set the global opts
    gopts.prog = prog

    prog.addopt('memlimit','2g')    
   
    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"
        
    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')        
        
    prog.addopt('file',os.path.join(mypath,'util.py'))
    
    prog.addopt('input',mat)
    matname,matext = os.path.splitext(mat)
    
    gopts.getstrkey('reduce_schedule','1')
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))
        
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
