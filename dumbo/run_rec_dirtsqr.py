"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
This is a script to run the recursive Direct TSQR algorithm.

See options:
     python run_rec_dirtsqr.py --help

Example usage:
     python run_rec_dirtsqr.py --input=A_800M_10.bseq \
            --ncols=10 --svd=2 --schedule=100,100,100 \
            --hadoop=$HADOOP_INSTALL --local_output=tsqr-tmp \
            --output=DIRTSQR_TESTING

This script is designed to run on ICME's MapReduce cluster, icme-hadoop1.
"""

import os
import shutil
import subprocess
import sys
import time
import util
from optparse import OptionParser

# Parse command-line options
#
# TODO(arbenson): use argparse instead of optparse when icme-hadoop1 defaults
# to python 2.7
parser = OptionParser()
parser.add_option('-i', '--input', dest='input', default='',
                  help='input matrix')
parser.add_option('-o', '--output', dest='out', default='',
                  help='base string for output of Hadoop jobs')
parser.add_option('-l', '--local_output', dest='local_out', default='direct_out_tmp',
                  help='Base directory for placing local files')
parser.add_option('-t', '--times_output', dest='times_out', default='times',
                  help='file for storing command times')
parser.add_option('-n', '--ncols', type='int', dest='ncols', default=0,
                  help='number of columns in the matrix')
parser.add_option('-s', '--schedule', dest='sched', default='100,100,100',
                  help='comma separated list of number of map tasks to use for'
                       + ' the three jobs')
parser.add_option('-H', '--hadoop', dest='hadoop', default='',
                  help='name of hadoop for Dumbo')
parser.add_option('-q', '--quiet', action='store_false', dest='verbose',
                  default=True, help='turn off some statement printing')

(options, args) = parser.parse_args()
cm = util.CommandManager(verbose=options.verbose)

# Store options in the appropriate variables
in1 = options.input
if in1 == '':
  cm.error('no input matrix provided, use --input')

out = options.out
if out == '':
  # TODO(arbenson): make sure in1 is clean
  out = in1 + '_DIRECT'

local_out = options.local_out
out_file = lambda f: local_out + '/' + f
if os.path.exists(local_out):
  shutil.rmtree(local_out)
os.mkdir(local_out)

times_out = options.times_out

ncols = options.ncols
if ncols == 0:
  cm.error('number of columns not provided, use --ncols')

sched = options.sched
try:
  sched = [int(s) for s in sched.split(',')]
  sched[2]
except:
  cm.error('invalid schedule provided')

hadoop = options.hadoop
if hadoop[0] == '$':
	hadoop = os.environ[hadoop[1:]]

R_labelled_out = out + '_R_LABELLED'
out1 = out + '_1'

# Now run the MapReduce jobs
cm.run_dumbo('dirtsqr1.py', hadoop, ['-mat ' + in1, '-output ' + out1,
                                     '-nummaptasks %d' % sched[0],
                                     '-libjar feathers.jar'])

# Two step recursion
cm.run_dumbo('RLabeller.py', hadoop, ['-mat ' + out1 + '/R_*',
                                      '-output ' + R_labelled_out])

# Recursive step
rec_out = 'RECURSIVE_DIRTSQR_TEST'
cmd = 'python run_dirtsqr.py '
cmd += '--input=' + R_labelled_out + ' '
cmd += '--ncols=' + str(ncols) + ' '
cmd += '--schedule=80,80,80 '
cmd += '--times_output=recursive_times '
cmd += '--output=' + rec_out + ' '
cmd += '--hadoop=' + hadoop + ' '

cm.exec_cmd(cmd)

# Run the Q grouper
Q_rec_out = 'DIRTSQR_Q_RECURSIVE'
cm.run_dumbo('QGrouper.py', hadoop, ['-mat ' + rec_out + '_3',
                                     '-output ' + Q_rec_out,
                                     '-ncols ' + str(ncols)])

# Finally, generate Q
in3 = out1 + '/Q_*'
cm.run_dumbo('dirtsqr3_rec.py', hadoop, ['-mat ' + in3,
                                         '-rec_mat ' + Q_rec_out,
                                         '-output ' + out + '_3',
                                         '-ncols ' + str(ncols),
                                         '-reducetasks ' + str(sched[2]),
                                         '-nummaptasks %d' % sched[2]])

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times) + '\n')
  f.close
except:
  print str(cm.times)
