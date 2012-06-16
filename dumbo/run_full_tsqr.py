#!/usr/bin/env python

"""
This is a script to run the Full TSQR algorithm with direct computation
of the matrix Q.

usage:

    python run_full_tsqr.py matrix_name [map_schedule] [output_name]

matrix_name is the name of the input file that is stored in the HDFS.

map_schedule is a string representation of a comma-separated list of length 
three. The list is used to set Hadoop's nummaptasks option for each phase.  For
example, "50, 60, 40" will set nummaptasks to 50 for the first phase, 60 for the
second phase, and 40 for the third phase.  The algorithm is designed to scale
for map tasks, so each part of the schedule should be as large as possible.  If
this argument is not provided, then "100, 100, 100" is used.  Note that the
actual number of map tasks used may differ from the option provided to Hadoop.

output_name is used to derive the name of the outpute directories.
The outputs of the three jobs will be stored in output_name_1,
output_name_2, and output_name_3.

This script is designed to run on ICME's MapReduce cluster, icme-hadoop1.

Austin R. Benson arbenson@gmail.com
Copyright (c) 2012
"""

import sys
import time
import subprocess
from optparse import OptionParser

verbose = True
times = []
split = '-'*60

# print error messages and exit with failure
def error(msg):
  print msg
  sys.exit(1)

# simple wrapper around printing with verbose option
def output(msg):
  if verbose:
    print msg

# simple wrapper for executing command line programs
def exec_cmd(cmd):
  output('(command is: %s)' % (cmd))
  output(split)
  t0 = time.time()
  retcode = subprocess.call(cmd,shell=True)
  times.append(time.time() - t0)
  # TODO(arbenson): make it more obvious when something fails
  return retcode

# simple wrapper for parsing a sequence file
def parse_seq_file(inp):
  parse_cmd = 'python hyy-python-hadoop/examples/SequenceFileReader.py %s > %s' % (inp, inp + '.out')
  exec_cmd(parse_cmd)

# simple wrapper for running dumbo scripts with options provided as a list
def run_dumbo(script, hadoop='', opts=[]):
  cmd = 'dumbo start ' + script
  if hadoop != '':
    cmd += ' -hadoop '
    cmd += hadoop
  for opt in opts:
    cmd += ' '
    cmd += opt
  exec_cmd(cmd)


# Parse command-line options
#
# TODO(arbenson): use argparse instead of optparse when icme-hadoop1 defaults
# to python 2.7
parser = OptionParser()
parser.add_option('-i', '--input', dest='input', default='',
                  help='input matrix')
parser.add_option('-o', '--output', dest='out', default='',
                  help='base string for output of Hadoop jobs')
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


# Store options in the appropriate variables
in1 = options.input
if in1 == '':
  error('no input matrix provided, use --input')

out = options.out
if out == '':
  # TODO(arbenson): make sure in1 is clean
  out = in1 + '_FULL'

ncols = options.ncols
if ncols == 0:
  error('number of columns not provided, use --ncols')

svd_opt = options.svd
verbose = options.verbose

sched = options.sched
try:
  sched = [int(s) for s in sched.split(',')]
  sched[2]
except:
  error('invalid schedule provided')


# Now run the MapReduce jobs
out1 = out + '_1'
run_dumbo('full1.py', 'icme-hadoop1', ['-mat ' + in1, '-output ' + out1,
                                       '-nummaptasks %d' % sched[0],
                                       '-libjar feathers.jar'])

out2 = out + '_2'
run_dumbo('full2.py', 'icme-hadoop1', ['-mat ' + out1 + '/R_*', '-output ' + out2,
                                       '-svd ' + str(svd_opt),
                                       '-nummaptasks %d' % sched[1],
                                       '-libjar feathers.jar'])

# Q2 file needs parsing before being distributed to phase 3
Q2_file = out2 + '_Q2.txt'

rm_cmd = 'rm -rf %s %s' % (Q2_file, Q2_file + '.out')
exec_cmd(rm_cmd)

copy_cmd = 'hadoop fs -copyToLocal %s/Q2/part-00000 %s' % (out2, Q2_file)
exec_cmd(copy_cmd)

parse_seq_file(Q2_file)

in3 = out1 + '/Q_*'
run_dumbo('full3.py', 'icme-hadoop1', ['-mat ' + in3, '-output ' + out + '_3',
                                       '-ncols ' + str(ncols),
                                       '-q2path ' + Q2_file + '.out',
                                       '-nummaptasks %d' % sched[2],
                                       '-libjar feathers.jar'])

if svd_opt == 2:
  small_U_file = out2 + '_U.txt'

  rm_cmd = 'rm -rf %s %s' % (small_U_file, small_U_file + '.out')
  exec_cmd(rm_cmd)

  copy_cmd = 'hadoop fs -copyToLocal %s/U/part-00000 %s' % (out2, small_U_file)
  exec_cmd(copy_cmd)

  parse_seq_file(small_U_file)

  # We need an addition TS matrix multiply to get the left singular vectors
  out4 = out + '_4'

  run_dumbo ('TSMatMul.py', 'icme-hadoop1', ['-mat ' + out + '_3', '-output ' + out4,
                                             '-mpath ' + small_U_file + '.out',
                                             '-nummaptasks %d' % sched[2]])

output('times: ' + str(times))
