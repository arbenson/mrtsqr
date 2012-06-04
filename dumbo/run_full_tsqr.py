#!/usr/bin/env python

"""
This is a script to run the Full TSQR algorithm with direct computation
of the matrix Q.

usage:

    python run_full_tsqr.py matrix_name [map_schedule] [output_name]

matrix_name is the name of the input file that is stored in the HDFS.

map_schedule is a string representation of a comma-separated list of length three.
The list is used to set Hadoop's nummaptasks option for each phase.  For example,
"50, 60, 40" will set nummaptasks to 50 for the first phase, 60 for the second
phase, and 40 for the third phase.  The algorithm is designed to scale for map
tasks, so each part of the schedule should be as large as possible.  If this
argument is not provided, then "100, 100, 100" is used.  Note that the actual
number of map tasks used may differ from the option provided to Hadoop.

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

try:
    in1 = sys.argv[1]
    ncols = sys.argv[2]
except:
    print 'usage: python run_full_tsqr.py input ncols [svd_opt] [schedule] [output]'
    sys.exit(-1)

try:
    """
    SVD option
    0: no SVD
    1: compute the singular values (R = USVt)
    2: compute the singular vectors as well as QR
    """
    svd_opt = int(sys.argv[3])
except:
    svd_opt = 0    

try:
    sched = sys.argv[4]
    sched = [int(s) for s in sched.split(',')]
    sched[2]
    print 'schedule: ' + str(sched[0:3])
except:
    sched = [100,100,100]
    print 'schedule: ' + str(sched)    

try:
    out = sys.argv[5]
except:
    # TODO(arbenson): make sure in1 is clean
    out = in1 + '_FULL'

times = []
split = '-'*60

def exec_cmd(cmd):
  print '(command is: %s)' % (cmd)
  print split
  t0 = time.time()
  retcode = subprocess.call(cmd,shell=True)
  times.append(time.time() - t0)
  return retcode

def parse_seq_file(inp):
    parse_cmd = 'python hyy-python-hadoop/examples/SequenceFileReader.py %s > %s' % (inp, inp + '.out')
    exec_cmd(parse_cmd)

def run_dumbo(script, hadoop='', opts=[]):
    cmd = 'dumbo start ' + script
    if hadoop != '':
        cmd += ' -hadoop '
        cmd += hadoop
    for opt in opts:
        cmd += ' '
        cmd += opt

    exec_cmd(cmd)


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

print 'times: ' + str(times)
