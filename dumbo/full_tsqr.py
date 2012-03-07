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
TODO:

1. Add in the join keys
2. Make sure join keys and multiple output files play nicely together
3. Test, test, test
4. Benchmark against AR^{-1} + IR
"""

class FullTSQRMap1(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.nrows = 0
        self.keys = []
        self.data = []
        self.ncols = None
        self.mapper_id = uuid.uuid1()
    
    def QR(self):
        A = numpy.array(self.data)
        return numpy.linalg.qr(A)
    
    def collect(self,key,value):
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            assert(len(value) == self.ncols)

        self.keys.append(key)        
        self.data.append(value)

        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        Q, R = self.QR()
        self.counters['rows processed'] += self.nrows%50000

        for i, row in enumerate(Q):
            yield ("Q_%d" % self.mapper_id, self.keys[i]), row
        for i, row in enumerate(R):
            yield ("R_%d" % self.mapper_id, str(self.mapper_id)), row

    def __call__(self, data):
        for key,value in data:
            if isinstance(value, str):
                # handle conversion from string
                value = [float(p) for p in value.split()]
            self.collect(key,value)
                
        # finally, output data
        for key,val in self.close():
            yield key,val


# FullTSQRRed1 is identity


"""
FullTSQRRed2

Takes all of the intermediate Rs

Computes [R_1, ..., R_n] = Q2R_{final}

Output: Q2, R_{final}

where Q2 is a list of key value pairs.

Each key corresponds to a mapperid from stage 1 and that keys value is the
Q2 matrix corresponding to that mapper_id
"""

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



class FullTSQRMap3(dumbo.backends.common.MapRedBase):
    def __init__(self):
        # TODO implement this correctly
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
        for key, values in data:
            if isinstance(value, str):
                # handle conversion from string
                value = [float(p) for p in value.split()]
            self.collect(key1, key2, value)            

        for key, val in self.close():
            yield key, val
