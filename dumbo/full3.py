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

import numpy
import numpy.linalg

import util
import uuid

import dumbo
import dumbo.backends.common

from dumbo import opt
from dumbo.lib import MultiMapper, JoinReducer
from dumbo.decor import primary, secondary

# create the global options structure
gopts = util.GlobalOptions()

class DataFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

"""
FullTSQRRed3
------------

input: Q1 as <mapper_id, [row] + [row_id]>
input: Q2 comes attached as a text file, which is then parsed on the fly

output: Q as <row_id, row>
"""
class FullTSQRRed3(dumbo.backends.common.MapRedBase):
    def __init__(self, q2path):
        # TODO implement this
        self.Q1_data = {}
        self.row_keys = {}
        self.Q2_data = {}
        self.Q_final_out = {}
        self.ncols = None
        self.q2path = q2path

    def parse_q2(self):
        f = open(self.q2path)
        for line in f:
            if len(line) > 5:
                ind1 = line.find("'")
                ind2 = line.rfind("'")
                key = line[ind1+1:ind2]           
                line = line[ind2+3:]
                line = line.strip()
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
            self.parse_q2()
        
        if key1 not in self.Q1_data:
            self.Q1_data[key1] = []
            assert(key1 not in self.row_keys)
            self.row_keys[key1] = []

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
                yield self.row_keys[key][i], row.tolist()

    def __call__(self,data):
        for key1, values in data:
            for value in values:
                key2 = value[-1]
                value = value[0:-1]
                self.collect(key1, key2, value)            

        for key, val in self.close():
            yield key, val
    

def runner(job):
    mapper = "org.apache.hadoop.mapred.lib.IdentityMapper"
    q2path = gopts.getstrkey('q2path')
    reducer = FullTSQRRed3(q2path)
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(100))])

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

    q2path = prog.delopt('q2path')
    if not q2path:
        return "'q2path' not specified"
    prog.addopt('file',os.path.join(mypath,q2path))

    gopts.getstrkey('q2path', q2path)    
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
