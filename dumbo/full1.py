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
FullTSQRMap1
--------------

Input: <key, value> pairs representing <row id, row> in the matrix A

Output:
  1. R matrix: <mapper id, row>
  2. Q matrix: <mapper id, row + [row_id]>
"""
@opt("getpath", "yes")
class FullTSQRMap1(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.nrows = 0
        self.keys = []
        self.data = []
        self.ncols = None        
        self.mapper_id = uuid.uuid1().hex
        self.isreducer=False
        self.unpacker = None
        self.isfinal = False

    
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
            #yield self.mapper_id, row

        for i, row in enumerate(self.R):
            yield ("R_%s" % str(self.mapper_id), self.mapper_id), row                    
            #yield self.mapper_id, row

    def deduce_string_type(self, val):
        # first check for TypedBytes list/vector
        try:
            [float(p) for p in val.split()]
        except:
            if len(val) == 0: return False
            if len(val)%8 == 0:
                ncols = len(val)/8
                # check for TypedBytes string
                try:
                    val = list(struct.unpack('d'*ncols, val))
                    self.ncols = ncols
                    self.unpacker = struct.Struct('d'*ncols)
                    return True
                except struct.error, serror:
                    # no idea what type this is!
                    raise DataFormatException("Data format is not supported.")
            else:
                raise DataFormatException("Number of data bytes ({0}) is not a multiple of 8.".format(len(val)))
            
    def __call__(self,data):
        deduced = False
        if self.isreducer == False:
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
                
        else:
            # determine the data format
            for key,values in data:
                for value in values:
                    if not deduced:
                        deduced = self.deduce_string_type(value)
                    if self.unpacker is not None:
                        val = self.unpacker.unpack(value)
                        self.collect(key,val)
                    else:
                        self.collect(key,value)
        # finally, output data
        for key,val in self.close():
            yield key, val

def runner(job):
    mapper = FullTSQRMap1()
    reducer="org.apache.hadoop.mapred.lib.IdentityReducer"
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
