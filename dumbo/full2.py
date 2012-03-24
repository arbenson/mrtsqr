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
    def __init__(self):
        self.R_data = {}
        self.key_order = []
        self.Q2 = None

    def add_R(self, key, value):
        if key not in self.R_data:
            print >> sys.stderr, 'adding key: %s'%(str(key))
            self.R_data[key] = []
        row = [float(val) for val in value]
        self.R_data[key].append(row)

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
    

def runner(job):
    mapper = "org.apache.hadoop.mapred.lib.IdentityMapper"
    reducer = FullTSQRRed2()
    job.additer(mapper=mapper,reducer=reducer,opts=[('numreducetasks',str(1))])

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
