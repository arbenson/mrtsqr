#!/usr/bin/env python

"""
This is a script to run the C++ implementation of the Full TSQR algorithm
with direct computation of the matrix Q.

See options:
     python run_full_tsqr_cxx.py --help

Example usage:
     python run_full_tsqr_cxx.py --input=A_800M_10.bseq \
            --ncols=10 --schedule=100,100,100 \
            --local_output=tsqr-tmp --output=FULL_TESTING

This script is designed to run on ICME's MapReduce cluster, icme-hadoop1.

Austin R. Benson     arbenson@stanford.edu
David F. Gleich
Copyright (c) 2012
"""

import os
import shutil
import subprocess
import sys
import time
from optparse import OptionParser
lib_path = os.path.abspath('../dumbo')
sys.path.append(lib_path)
import util

# Parse command-line options
#
# TODO(arbenson): use argparse instead of optparse when icme-hadoop1 defaults
# to python 2.7
parser = OptionParser()
parser.add_option('-i', '--input', dest='input', default='',
                  help='input matrix')
parser.add_option('-o', '--output', dest='out', default='',
                  help='base string for output of Hadoop jobs')
parser.add_option('-l', '--local_output', dest='local_out', default='full_out_tmp',
                  help='Base directory for placing local files')
parser.add_option('-t', '--times_output', dest='times_out', default='times',
                  help='Base directory for placing local files')
parser.add_option('-n', '--ncols', type='int', dest='ncols', default=0,
                  help='number of columns in the matrix')
parser.add_option('-s', '--schedule', dest='sched', default='100,100,100',
                  help='comma separated list of number of map tasks to use for'
                       + ' the three jobs')

# TODO(arbenson): add option that computes singular vectors but not the Q in
# QR.  This will be the go-to option for computing the SVD of a
# tall-and-skinny matrix.
parser.add_option('-x', '--svd', type='int', dest='svd', default=0,
                  help="""0: no SVD computed ;
1: compute the singular values (R = USV^t) ;
2: compute the singular vectors as well as QR
"""
)
parser.add_option('-q', '--quiet', action='store_false', dest='verbose',
                  default=True, help='turn off some statement printing')

(options, args) = parser.parse_args()
cm = util.CommandManager(verbose=options.verbose)

STREAMING_JAR='/usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u4.jar'

# Store options in the appropriate variables
in1 = options.input
if in1 == '':
  cm.error('no input matrix provided, use --input')

out = options.out
if out == '':
  # TODO(arbenson): make sure in1 is clean
  out = in1 + '_FULL'

local_out = options.local_out
out_file = lambda f: local_out + '/' + f
if os.path.exists(local_out):
  shutil.rmtree(local_out)
os.mkdir(local_out)

times_out = options.times_out

ncols = options.ncols
if ncols == 0:
  cm.error('number of columns not provided, use --ncols')

svd_opt = options.svd

sched = options.sched
try:
  sched = [int(s) for s in sched.split(',')]
  sched[2]
except:
  cm.error('invalid schedule provided')

def form_cmd(hadoop_opts):
  cmd = 'hadoop jar %s -libjars feathers.jar ' % STREAMING_JAR
  for opt_type in hadoop_opts:
    for opt in hadoop_opts[opt_type]:
      cmd += '-%s %s ' % (opt_type, opt)
  return cmd

def run_step(hadoop_opts):
  cm.exec_cmd('hadoop fs -rmr ' + hadoop_opts['output'][0])
  cm.exec_cmd(form_cmd(hadoop_opts))

hadoop_opts = {'jobconf': ['mapreduce.job.name=tsqr_cxx',
                           'stream.map.input=typedbytes',
                           'stream.reduce.input=typedbytes',
                           'stream.map.output=typedbytes',
                           'stream.reduce.output=typedbytes',],
               'inputformat': ['org.apache.hadoop.streaming.AutoInputFormat'],
               'outputformat': ['fm.last.feathers.output.MultipleSequenceFiles'],
               'file': ['tsqr', 'tsqr_wrapper.sh'],
               'input': [in1],
               'mapper': ["'./tsqr_wrapper.sh direct 1'"],
               'reducer': ['org.apache.hadoop.mapred.lib.IdentityReducer'],
               'numReduceTasks': ['0'],
               }

# Now run the MapReduce jobs
out1 = out + '_1'
hadoop_opts['output'] = [out1]
jobconf = [x for x in hadoop_opts['jobconf']]
hadoop_opts['jobconf'] = jobconf + ['mapred.map.tasks=%d' % sched[0]]
run_step(hadoop_opts)

out2 = out + '_2'
hadoop_opts['input'] = [out1 + '/R_*']
hadoop_opts['output'] = [out2]
hadoop_opts['mapper'] =  ['org.apache.hadoop.mapred.lib.IdentityMapper']
hadoop_opts['reducer'] =  ["'./tsqr_wrapper.sh direct 2 %d'" % ncols]
hadoop_opts['numReduceTasks'] = ['1']
hadoop_opts['jobconf'] = jobconf + ['mapred.map.tasks=%d' % sched[1]]
run_step(hadoop_opts)

# Q2 file needs parsing before being distributed to phase 3
Q2_file = out_file('Q2.txt')

if os.path.exists(Q2_file):
  os.remove(Q2_file)

if os.path.exists(Q2_file + '.out'):
  os.remove(Q2_file + '.out')

cm.copy_from_hdfs(out2 + '/Q2', Q2_file)
cm.parse_seq_file(Q2_file)

out3 = out + '_3'
hadoop_opts['input'] = [out1 + '/Q_*']
hadoop_opts['output'] = [out3]
hadoop_opts['file'] += [Q2_file + '.out']
hadoop_opts['mapper'] =  ["'./tsqr_wrapper.sh direct 3 %d'" % ncols]
hadoop_opts['reducer'] = ['org.apache.hadoop.mapred.lib.IdentityReducer']
hadoop_opts['outputformat'] = ['org.apache.hadoop.mapred.SequenceFileOutputFormat']
hadoop_opts['numReduceTasks'] = ['0']
hadoop_opts['jobconf'] = jobconf + ['mapred.map.tasks=%d' % sched[2]]
run_step(hadoop_opts)

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times) + '\n')
  f.close
except:
  pass
