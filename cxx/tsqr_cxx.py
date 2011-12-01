#!/usr/bin/env python

"""
tsqr_cxx.py
===========

Submit the C++ version of tsqr to the hadoop streaming system.
"""
__author__ = 'David F. Gleich'

"""
History
-------
:2011-01-30: Initial coding

Todo
----
* Add code to find hadoop
* Add code to find hadoop streaming
* Handle directory offset
* Handle updating tsqr executable
"""

import sys
import os

import subprocess

import hadoopy

def get_args(argv):
    args = {}
    for i,arg in enumerate(argv):
        if arg[0] == '-':
            if i+1 < len(argv):
                val = argv[i+1]
            else:
                val = None
            args[arg[1:]] = val
    return args

if __name__=='__main__':
    #hadoop_dir = '/home/mrhadoop/hadoop-0.21.0/'
    #streaming_jar = hadoop_dir + 'mapred/contrib/streaming/hadoop-0.21.0-streaming.jar'

    hadoop_dir = '/usr/common/tig/hadoop/hadoop-0.20.2+228/'
    streaming_jar = hadoop_dir + 'contrib/streaming/hadoop-0.20.2+228-streaming.jar'
    
    args = get_args(sys.argv[1:])
    
    hadoop_args = []
    
    if 'mat' in args:
        mat = args['mat']
    else:
        print >>sys.stderr, "Error: -mat not specified"

    input = mat
    matname,matext = os.path.splitext(mat)
    
    if 'output' in args:
        output = args['output']
    else:
        output = '%s-qrr%s'%(matname,matext)

    if 'rows_per_rec' in args:
        rows_per_rec = args['rows_per_rec']
    else:
        rows_per_rec = 1

        
    jobname = 'tsqr_cxx ' + matname
    
    if 'blocksize' in args:
        blocksize = int(args['blocksize'])
    else:
        blocksize = 3

    if 'split_size' in args:
        hadoop_args.extend(['-jobconf',
            'mapreduce.input.fileinputformat.split.minsize='+
            args['split_size']])
            
            
    if 'big_mem' in args:
        hadoop_args.extend(['jobconf',
            'mapred.child.java.opts="-Xmx%s"'%(args['big_mem'])])
    
    hadoop_args.extend(['-io', 'typedbytes'])
    hadoop_args.extend(['-file', 'tsqr'])
    hadoop_args.extend(['-file', '/usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_core.so'])
    hadoop_args.extend(['-file', '/usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_intel_lp64.so'])
    hadoop_args.extend(['-file', '/usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_sequential.so'])
    hadoop_args.extend(['-file', '/usr/common/usg/mkl/10.2.2.025/lib/em64t/libiomp5.so'])
    hadoop_args.extend(['-file', 'mkl_and_tsqr.sh'])
    hadoop_args.extend(['-reducer', "'./mkl_and_tsqr.sh reduce %i'"%(blocksize)])
    #hadoop_args.extend(['-combiner', "'./mkl_and_tsqr.sh reduce %i'"%(blocksize)])
    hadoop_args.extend(['-outputformat', "'org.apache.hadoop.mapred.SequenceFileOutputFormat'"])
    hadoop_args.extend(['-inputformat', "'org.apache.hadoop.streaming.AutoInputFormat'"])

    # now we would handle the reduce schedule, or whatever else is
    schedule = args.get('reduce_schedule','1')
    steps = schedule.split(',')
    steps = [int(s) for s in steps]
    
    for i,step in enumerate(steps):
        cur_args = [arg for arg in hadoop_args]
        
        if i>0:
            input = curoutput
            cur_args.extend(['-mapper', "'org.apache.hadoop.mapred.lib.IdentityMapper'"])    
        else:
            cur_args.extend(['-mapper', "'./mkl_and_tsqr.sh map %i'"%(blocksize)])    
        
        if i+1==len(steps):
            curoutput = output
        else:
            curoutput = output+"_iter%i"%(i+1)
            
        
        cur_args.extend(['-jobconf',"'mapreduce.job.name="+jobname+
            " (%i/%i)'"%(i+1,len(steps))])
        cur_args.extend(['-input',"'"+input+"'"])
        cur_args.extend(['-output',"'"+curoutput+"'"])
        cur_args.extend(['-numReduceTasks', "'%i'"%(int(step))])
    
        cmd = ['hadoop','jar',streaming_jar]
        cmd.extend(cur_args)
    
        print "Running Hadoop Command:"
        print
        print ' '.join(cmd) 
        print
        print "End Hadoop Command"
        
        if hadoopy.exists(curoutput):
            print "Removing %s"%(curoutput)
            hadoopy.rm(curoutput)

        subprocess.check_call(' '.join(cmd),shell=True)
