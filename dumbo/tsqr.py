#!/usr/bin/env dumbo

"""
tsqr.py
===========

Implement a tsqr algorithm using dumbo and numpy
"""

import sys
import os
import time
import random
import struct

import numpy
import numpy.linalg

import util

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class DataFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class SerialTSQR(dumbo.backends.common.MapRedBase):
    def __init__(self,blocksize=3,keytype='random',isreducer=False,isfinal=False):
        self.blocksize=blocksize
        if keytype=='random':
            self.keyfunc = lambda x: random.randint(0, 4000000000)
        elif keytype=='first':
            self.keyfunc = self._firstkey
        else:
            raise Error("Unkonwn keytype %s"%(keytype))
        self.first_key = None
        self.isreducer=isreducer
        self.nrows = 0
        self.data = []
        self.ncols = None
        self.unpacker = None
        self.isfinal = isfinal
    
    def _firstkey(self, i):
        if isinstance(self.first_key, (list,tuple)):
            return (util.flatten(self.first_key),i)
        else:
            return (self.first_key,i)
    
    def array2list(self,row):
        return [float(val) for val in row]

    def QR(self):
        A = numpy.array(self.data)
        return numpy.linalg.qr(A,'r')
        
    def compress(self):
        """ Compute a QR factorization on the data accumulated so far. """
        if self.ncols is None:
            return
        if len(self.data) < self.ncols:
            return

        t0 = time.time()
        R = self.QR()
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and re-initialize to R
        self.data = []
        for row in R:
            self.data.append(self.array2list(row))
                        
    def collect(self,key,value):
        if len(self.data) == 0:
            self.first_key = key
        
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            assert(len(value) == self.ncols)

        self.data.append(value)
        self.nrows += 1
        
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['QR Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        for i,row in enumerate(self.data):
            key = self.keyfunc(i)
            # If we are using TypedBytes String and this is not the final output,
            # then continue to use that format
            if self.unpacker is not None and not self.isfinal:
                yield key, struct.pack('d'*len(row),*row)
            else:
                yield key, row

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
                raise DataFormatException("Number of data bytes is not a multiple of 8.")
                    
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
    #niter = int(os.getenv('niter'))
    
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper="org.apache.hadoop.mapred.lib.IdentityMapper",
                reducer="org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=[('numreducetasks',str(nreducers))])
            
        else:
            nreducers = int(part)
            if i==0:
                mapper = SerialTSQR(blocksize=blocksize,isreducer=False,isfinal=False)
                isfinal=False
            else:
                mapper = 'org.apache.hadoop.mapred.lib.IdentityMapper'
                isfinal=True
            job.additer(mapper=mapper,
                        reducer=SerialTSQR(blocksize=blocksize,isreducer=True,isfinal=isfinal),
                        opts=[('numreducetasks',str(nreducers))])
    

def starter(prog):
    
    print "running starter!"
    
    mypath =  os.path.dirname(__file__)
    print "my path: " + mypath
    
    # set the global opts
    gopts.prog = prog

    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"

    # add numreps copies of the input
    numreps = prog.delopt('repetition')
    if not numreps:
        numreps = 1
    for i in range(int(numreps)):
        prog.addopt('input',mat)
        
    prog.addopt('memlimit','2g')
    
    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')
        
    prog.addopt('file',os.path.join(mypath,'util.py'))

    matname,matext = os.path.splitext(mat)
    
    gopts.getintkey('blocksize',3)
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
