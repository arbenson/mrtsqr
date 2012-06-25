#!/usr/bin/env dumbo

"""
Full mrtsqr

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys
import time
import struct
import uuid

import numpy
import numpy.linalg

import util
import mrmc

import dumbo
import dumbo.backends.common
from dumbo import opt

"""
FullTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""
@opt("getpath", "yes")
class FullTSQRMap1(mrmc.MatrixHandler):
    def __init__(self):
        mrmc.MatrixHandler.__init__(self)
        self.keys = []
        self.data = []
        self.mapper_id = uuid.uuid1().hex
    
    def collect(self,key,value):
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            assert(len(value) == self.ncols)

        # TODO(arbenson): generalize to handle non-integer keys
        self.keys.append(int(key))
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

        QR = numpy.linalg.qr(numpy.array(self.data))
        Q = QR[0].tolist()

        yield ("R_%s" % str(self.mapper_id), self.mapper_id), QR[1].tolist()

        for i, row in enumerate(Q):
            Q[i].append(self.keys[i])
            if i%50000 == 0:
                self.counters['Q rows processed'] += 50000
        
        flat_Q = [entry for row in Q for entry in row]
        yield ("Q_%s" % str(self.mapper_id), self.mapper_id), struct.pack('d'*len(flat_Q), *flat_Q)

    def __call__(self,data):
        self.collect_data(data)
        for key,val in self.close():
            yield key, val


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
@opt("getpath", "yes")
class FullTSQRRed2(dumbo.backends.common.MapRedBase):
    def __init__(self, compute_svd=False):
        self.R_data = {}
        self.key_order = []
        self.Q2 = None
        self.compute_svd = compute_svd

    def collect(self, key, value):
        assert(key not in self.R_data)
        data = []
        for row in value:
            data.append([float(val) for val in row])
        self.R_data[key] = data

    def close_R(self):
        data = []
        for key in self.R_data:
            data += self.R_data[key]
            self.key_order.append(key)
        A = numpy.array(data)
        QR = numpy.linalg.qr(A)        
        self.Q2 = QR[0].tolist()
        self.R_final = QR[1].tolist()
        for i, row in enumerate(self.R_final):
            yield ("R_final", i), row
        if self.compute_svd:
            U, S, Vt = numpy.linalg.svd(self.R_final)
            S = numpy.diag(S)
            for i, row in enumerate(U):
                yield ("U", i), row
            for i, row in enumerate(S):
                yield ("Sigma", i), row
            for i, row in enumerate(Vt):
                yield ("Vt", i), row

    def close_Q(self):
        num_rows = len(self.Q2)
        rows_to_read = num_rows / len(self.key_order)

        ind = 0
        key_ind = 0
        local_Q = []
        for row in self.Q2:
            local_Q.append(row)
            ind += 1
            if (ind == rows_to_read):
               flat_Q = [entry for row in local_Q for entry in row]
               yield ("Q2", self.key_order[key_ind]), flat_Q
               key_ind += 1
               local_Q = []
               ind = 0

    def __call__(self,data):
        for key,values in data:
                for value in values:
                    self.collect(key, value)

        for key, val in self.close_R():
            yield key, val
        for key, val in self.close_Q():
            yield key, val


"""
FullTSQRMap3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class FullTSQRMap3(dumbo.backends.common.MapRedBase):
    def __init__(self,q2path='q2.txt',ncols=10):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}
        self.ncols = ncols
        self.q2path = q2path

    def parse_q2(self):
        f = open(self.q2path)
        for line in f:
            if len(line) > 5:
                ind1 = line.find('(')
                ind2 = line.rfind(')')
                key = line[ind1+1:ind2]
                # lazy parsing: we only need the keys that we have
                if key not in self.Q1_data:
                    continue
                line = line[ind2+3:]
                line = line.lstrip('[').rstrip().rstrip(']')
                line = line.split(',')
                line = [float(v) for v in line]
                line = numpy.array(line)
                mat = numpy.reshape(line, (self.ncols, self.ncols))
                self.Q2_data[key] = mat
        f.close()

    # key1: unique mapper_id
    # key2: row identifier
    # value: row of Q1
    def collect(self, key1, key2, value):
        row = [float(val) for val in value]
        if self.ncols is None:
            self.ncols = len(row)
        
        if key1 not in self.Q1_data:
            self.Q1_data[key1] = []
            assert(key1 not in self.row_keys)
            self.row_keys[key1] = []

        self.Q1_data[key1].append(row)
        self.row_keys[key1].append(key2)

    def close(self):
        # parse the q2 file we were given
        self.parse_q2()
        
        for key in self.Q1_data:
            assert(key in self.row_keys)
            assert(key in self.Q2_data)
            Q1 = numpy.mat(self.Q1_data[key])
            Q2 = numpy.mat(self.Q2_data[key])
            Q_out = Q1*Q2
            for i, row in enumerate(Q_out.getA()):
                row = row.tolist()
                yield self.row_keys[key][i], struct.pack('d'*len(row), *row)

    def __call__(self,data):
        for key1, matrix in data:
            num_entries = len(matrix)/8
            mat = struct.unpack('d'*num_entries, matrix)
            mat = list(mat)
            mat = numpy.mat(mat)
            assert (num_entries % (self.ncols + 1) == 0)
            mat = numpy.reshape(mat, (num_entries/(self.ncols + 1), self.ncols + 1))
            for value in mat.tolist():
                key2 = value[-1]
                value = value[0:-1]
                self.collect(key1, key2, value)

        for key, val in self.close():
            yield key, val
