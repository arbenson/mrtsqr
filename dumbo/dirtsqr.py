#!/usr/bin/env dumbo

"""
Direct MapReduce TSQR

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys
import time
import struct
import uuid
import cPickle as pickle

import numpy
import numpy.linalg

import util
import mrmc

import dumbo
import dumbo.backends.common
from dumbo import opt

"""
DirTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""
@opt("getpath", "yes")
class DirTSQRMap1(mrmc.MatrixHandler):
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

        self.keys.append(key)
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows % 50000

        # if no data was passed to this task, we just return
        if len(self.data) == 0:
            return

        QR = numpy.linalg.qr(numpy.array(self.data))

        yield ("R_%s" % str(self.mapper_id), self.mapper_id), QR[1].tolist()

        flat_Q = [entry for row in QR[0] for entry in row]
        val1 = pickle.dumps(self.keys)
        val2 = struct.pack('d'*len(flat_Q), *flat_Q)
        val = ''.join([str(len(val1)) + '_', val1, val2])
        yield ("Q_%s" % str(self.mapper_id), self.mapper_id), val

    def __call__(self,data):
        self.collect_data(data)
        for key,val in self.close():
            yield key, val


"""
DirTSQRRed2
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
class DirTSQRRed2(dumbo.backends.common.MapRedBase):
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
DirTSQRMap3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class DirTSQRMap3(dumbo.backends.common.MapRedBase):
    def __init__(self,ncols,q2path='q2.txt',upath=None):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.ncols = ncols
        self.q2path = q2path
        self.u_data = None
        if upath is not None:
          self.u_data = []
          for row in util.parse_matrix_txt(upath):
            self.u_data.append(row)
          self.u_data = numpy.mat(self.u_data)

    def parse_q2(self):
        try:
            f = open(self.q2path, 'r')
        except:
            # We may be expecting only the file to be distributed
            # with the script
            f = open(self.q2path.split('/')[-1], 'r')        
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

    def collect(self, key, keys, value):
        self.Q1_data[key] = (keys, value)

    def close(self):
        # parse the q2 file we were given
        self.parse_q2()
        for key in self.Q1_data:
            assert(key in self.Q2_data)
            keys, Q1 = self.Q1_data[key]
            Q2 = self.Q2_data[key]
            if self.u_data is not None:
              Q2 = Q2 * self.u_data
            Q_out = Q1 * Q2
            for i, row in enumerate(Q_out.getA()):
                yield keys[i], struct.pack('d' * len(row), *row)

    def __call__(self, data):
        for key, val in data:
            ind = val.find('_')
            val1_len = int(val[:ind])
            keys = val[ind + 1:ind + 1 + val1_len]
            matrix = val[ind +1 +val1_len:]
            keys = pickle.loads(keys)
            num_entries = len(matrix) / 8
            assert (num_entries % self.ncols == 0)
            mat = struct.unpack('d' * num_entries, matrix)
            mat = numpy.mat(mat)
            mat = numpy.reshape(mat, (num_entries / self.ncols , self.ncols))
            self.collect(key, keys, mat)

        for key, val in self.close():
            yield key, val

class RLabeller(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.data = []

    def close(self):
        for pair in self.data:
            yield pair[0], pair[1]

    def __call__(self, data):
        for key, value in data:
            for i, row in enumerate(value):
                new_key = str(key) + '_' + str(i)
                row = [float(val) for val in row]
                row = struct.pack('d'*len(row), *row)
                self.data.append((new_key, row))
        
        for key, val in self.close():
            yield key, val

class QGrouper(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.data = []

    def close(self):
        for pair in self.data:
            yield pair[0], pair[1]

    def __call__(self, data):
        for key, value in data:
            new_key, num = key.split('_')
            val = pickle.dumps((value, int(num)))
            self.data.append((new_key, val))
        
        for key, val in self.close():
            yield key, val

class QGrouper2(dumbo.backends.common.MapRedBase):
    def __init__(self, ncols):
        self.ncols = ncols
        self.data = {}

    def close(self):
        for key in self.data:
            assert(None not in self.data[key])
            local_Q = self.data[key]
            flat_Q = [entry for row in local_Q for entry in row]
            val = 'Q2' + '_' + struct.pack('d' * (self.ncols ** 2), *flat_Q)
            yield key, val

    def collect(self, key, value, num):
        assert(num < self.ncols)
        if key not in self.data:
            self.data[key] = self.ncols * [None]

        row = struct.unpack('d' * self.ncols, value)
        self.data[key][num] = row

    def __call__(self, data):
        for key, values in data:
            for value in values:
                val, num = pickle.loads(value)
                self.collect(key, val, num)
        
        for key, val in self.close():
            yield key, val


"""
DirTSQRMap3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class DirTSQRRed3(dumbo.backends.common.MapRedBase):
    def __init__(self, ncols):
        # TODO implement this
        #self.Q1_data = {}
        #self.row_keys = {}
        #self.Q2_data = {}
        self.ncols = ncols
        self.Q1_data = None
        self.Q2_data = None

    def collect(self, key, keys, value):
        self.Q1_data = (keys, value)

    def collect_Q2(self, key, value):
        value = numpy.array(struct.unpack('d' * (self.ncols ** 2), value))
        self.Q2_data = numpy.reshape(value, (self.ncols, self.ncols))

    #def close(self):
    #    for key in self.Q1_data:
    #        assert(key in self.Q2_data)
    #        keys, Q1 = self.Q1_data[key]
    #        Q2 = self.Q2_data[key]
    #        Q_out = Q1 * Q2
    #        for i, row in enumerate(Q_out.getA()):
    #            yield keys[i], struct.pack('d' * len(row), *row)

    def flush(self):
        keys, Q1 = self.Q1_data
        Q2 = self.Q2_data
        Q_out = Q1 * Q2
        for i, row in enumerate(Q_out.getA()):
                yield keys[i], struct.pack('d' * len(row), *row)
        self.Q1_data = None
        self.Q2_data = None

    def __call__(self, data):
        for key, values in data:
            for val in values:
                ind = val.find('_')
                if val[:ind] == 'Q2':
                    mat = val[ind+1:]
                    self.collect_Q2(key, mat)
                else:
                    val1_len = int(val[:ind])
                    keys = val[ind + 1:ind + 1 + val1_len]
                    matrix = val[ind +1 +val1_len:]
                    keys = pickle.loads(keys)
                    num_entries = len(matrix) / 8
                    assert (num_entries % self.ncols == 0)
                    mat = struct.unpack('d' * num_entries, matrix)
                    mat = numpy.mat(mat)
                    mat = numpy.reshape(mat, (num_entries / self.ncols , self.ncols))
                    self.collect(key, keys, mat)

            for k, v in self.flush():
                yield k, v

