#!/usr/bin/env dumbo

import os
import util
import sys
import dumbo
import time
import numpy
import mrmc

gopts = util.GlobalOptions()

class BtAMapper:
    opts = [('addpath','yes')]
    def __init__(self, B_id):
        self.B_id = B_id

    def __call__(self,key_,value):
        path = key_[0]
        key = key_[1]
        if path.find(self.B_id) == -1:
            # this is A
            yield key, ('A', value)
        else:
            # this is B^t
            yield key, ('Bt', value)

class BtAReducer(dumbo.backends.common.MapRedBase):
    """ Now that we have U and An stored together, let's just combine them locally."""
    def __init__(self,blocksize=3):
        self.blocksize=blocksize        
        self.UtAn = None
        self.dataU = []
        self.dataA = []
        self.ncols = None
        self.nrows = 0
        
    def __call__(self,data):
        # this is always a reducer
        for key,values in data:
            for val in values:
                if val[0] == 'U':
                    self.collect(key,val[1],self.dataU)
                elif val[0] == 'A':
                    self.collect(key,val[1],self.dataA)
                else:
                    assert('This means you had a bug in the mapper')
        for key, val in self.close():
            yield key, val
   
    def array2list(self,row):
        return [float(val) for val in row]

    def compress(self):
        # Compute UtAn on the data accumulated so far
        if self.ncols is None:
            return
        # Wait until we get both aligned
        if len(self.dataU) != len(self.dataA):
            return       
        t0 = time.time()
        U_mat = numpy.mat(self.dataU)
        A_mat = numpy.mat(self.dataA)
        UtAn_flush = U_mat.T*A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)

        # reset data and add flushed update to local copy
        self.dataU = []
        self.dataA = []
        if self.UtAn == None:
            self.UtAn = UtAn_flush
        else:
            self.UtAn = self.UtAn + UtAn_flush

    def collect(self,key,value,subset):        
        if self.ncols == None:
            self.ncols = len(value)
            print >>sys.stderr, "Matrix size: %i columns"%(self.ncols)
        else:
            # TODO should we warn and truncate here?
            # No. that seems like something that will introduce
            # bugs.  Maybe we could add a "liberal" flag
            # for that.
            if len(value) != self.ncols:
                assert('Wrong input in terms of ncols length')
        
        subset.append(value)
        self.nrows += 1
        
        if len(subset)>self.blocksize*self.ncols:
            self.counters['UtAn Compressions'] += 1
            # compress the data
            self.compress()
            
        # write status updates so Hadoop doesn't complain
        if self.nrows%50000 == 0:
            self.counters['rows processed'] += 50000


    def close(self):
        assert(len(self.dataA) == len(self.dataU))
        self.counters['rows processed'] += self.nrows%50000
        self.compress()
        for ind, row in enumerate(self.UtAn.getA()):
            r = self.array2list(row)
            yield ind, r

def array_sum_reducer(key,values):
    for j,val in enumerate(values):
        if j==0: 
            arr = val
        else:
            assert(len(val) == len(arr))
            for k in xrange(len(arr)):
                arr[k] += val[k]
    yield key,arr   
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')
    schedule = int(schedule)

    job.additer(mapper=BtAMapper('DMD'), reducer=BtAReducer(blocksize=blocksize),
                opts=[('numreducetasks', str(schedule))])
    job.additer(mapper='org.apache.hadoop.mapred.lib.IdentityMapper',
                reducer=array_sum_reducer, 
                opts=[('numreducetasks','1')])


def starter(prog):
    # set the global opts
    gopts.prog = prog

    matB = prog.delopt('matB')
    if not matB:
        return "'matB' not specified'"
    matA = prog.delopt('matA')
    if not matA:
        return "'matA' not specified'"

    mrmc.starter_helper(prog)
        
    prog.addopt('input', matB)
    prog.addopt('input', matA)

    matname, matext = os.path.splitext(matA)
    
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')
    
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-BtA%s'%(matname,matext))
        
    gopts.save_params()
    
if __name__ == '__main__':
    import dumbo
    dumbo.main(runner, starter)
        
        
    
