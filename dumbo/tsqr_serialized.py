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

class SerialTSQR(dumbo.backends.common.MapRedBase):
    def __init__(self,blocksize=3,keytype='random',isreducer=False,ncols=10,nrows=None,use_null_input=False):
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
        self.use_null_input = use_null_input
        self.ncols = ncols
        if self.use_null_input:
            self.rmat = numpy.random.rand(nrows, self.ncols)

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
            if not len(value) == self.ncols:
                return

        self.compress_if_nec()
        
        self.data.append(value)
        self.nrows += 1
        
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000

    def fake_collect(self):
        for row in self.rmat:
            self.data.append(row)
        self.compress_if_nec()

    def compress_if_nec(self):
        if len(self.data)>self.blocksize*self.ncols:
            self.counters['QR Compressions'] += 1
            # compress the data
            self.compress()

    def close(self):
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        for i,row in enumerate(self.data):
            key = self.keyfunc(i)
            yield key, struct.pack('d'*len(row),*row)

    def __call__(self,data):
        if self.isreducer == False:
            # map job
            for key,value in data:
                if not self.use_null_input:
                    value = list(struct.unpack('d'*self.ncols, value))
                    self.collect(key,value)
                else:
                    self.fake_collect()
                
        else:
            for key,values in data:
                for value in values:
                    val = list(struct.unpack('d'*self.ncols, value))
                    self.collect(key,val)
        # finally, output data
        for key,val in self.close():
            yield key, val


def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    ncols = gopts.getintkey('ncols')
    nrows = gopts.getintkey('nrows')
    use_null_input = False
    if (nrows != -1):
        use_null_input = True
    if (nrows < -1 or nrows == 0):
        print "Number of null input rows is not a positive integer"
        sys.exit(1)
    
    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if part.startswith('s'):
            nreducers = int(part[1:])
            # these tasks should just spray data and compress
            job.additer(mapper="org.apache.hadoop.mapred.lib.IdentityMapper",
                reducer="org.apache.hadoop.mapred.lib.IdentityReducer",
                opts=[('numreducetasks',str(nreducers))])
            job.additer(mapper, reducer, opts=[('numreducetasks',str(nreducers))])
            
        else:
            nreducers = int(part)
            if i==0:
                mapper = SerialTSQR(blocksize=blocksize,isreducer=False,
                                    ncols=ncols,nrows=nrows,use_null_input=use_null_input)
            else:
                mapper = 'org.apache.hadoop.mapred.lib.IdentityMapper'
            job.additer(mapper=mapper,
                    reducer=SerialTSQR(blocksize=blocksize,isreducer=True,ncols=ncols),
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
        
    prog.addopt('memlimit','2g')
    
    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')
        
    prog.addopt('file',os.path.join(mypath,'util.py'))

    numreps = prog.delopt('repeat')
    if not numreps:
        numreps = 1
    for i in range(int(numreps)):
        prog.addopt('input',mat)

    rep = prog.delopt('replication')
    if rep:
        prog.addopt('jobconf', 'dfs.replication='+str(int(rep)))

    matname,matext = os.path.splitext(mat)
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    gopts.getintkey('ncols', 10)
    gopts.getintkey('nrows', -1)

    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))

    minsplit = prog.delopt('minsplit')
    if minsplit is not None:
        prog.addopt('jobconf', 'mapred.min.split.size='+str(minsplit))
        
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+str(splitsize))
        
    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
