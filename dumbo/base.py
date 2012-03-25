#!/usr/bin/env python

"""
base.py
===========

Austin R. Benson
copyright (c) 2012
"""

import os
import struct

import dumbo
import dumbo.backends.common

# some variables
ID_MAPPER = 'org.apache.hadoop.mapred.lib.IdentityMapper'
ID_REDUCER = 'org.apache.hadoop.mapred.lib.IdentityReducer'

class DataFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MatrixHandler(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.ncols = None
        self.unpacker = None

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
                    

def starter_helper(prog):
    print "running starter!"

    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath    

    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')

    prog.addopt('file',os.path.join(mypath,'util.py'))
    prog.addopt('file',os.path.join(mypath,'base.py'))

    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))
        
    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')

    prog.addopt('memlimit','2g')    
    
    mat = prog.delopt('mat')

    if mat:
        # add numreps copies of the input
        numreps = prog.delopt('repetition')
        if not numreps:
            numreps = 1
        for i in range(int(numreps)):
            prog.addopt('input',mat)
    
        return mat            
    else:
        return None


def add_splay_iteration(job, part):
    nreducers = int(part[1:])
    # these tasks should just spray data and compress
    job.additer(mapper = ID_MAPPER, reducer = ID_REDUCER,
                opts=[('numreducetasks',str(nreducers))])
    job.additer(mapper, reducer, opts=[('numreducetasks',str(nreducers))])    
