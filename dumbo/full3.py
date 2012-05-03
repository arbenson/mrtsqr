#!/usr/bin/env dumbo

"""
Austin R. Benson
David F. Gleich

copyright (c) 2012

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

# create the global options structure
gopts = util.GlobalOptions()

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
    

def runner(job):
    q2path = gopts.getstrkey('q2path')
    ncols = gopts.getintkey('ncols')
    mapper = FullTSQRMap3(q2path,ncols)
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
    gopts.getintkey('ncols',10)

    q2path = prog.delopt('q2path')
    if not q2path:
        return "'q2path' not specified"
    prog.addopt('file', os.path.join(os.path.dirname(__file__), q2path))

    gopts.getstrkey('q2path', q2path)    
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)

