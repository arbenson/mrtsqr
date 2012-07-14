#!/usr/bin/env python

"""
This is a script to run the Full TSQR algorithm with direct computation
of the matrix Q.

See options:
     python run_full_tsqr.py --help

Example usage:
     python run_full_tsqr.py --input=A_800M_10.bseq \
            --ncols=10 --svd=2 --schedule=100,100,100 \
            --hadoop=icme-hadoop1 --local_output=tsqr-tmp \
            --output=FULL_TESTING

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
parser.add_option('-l', '--local_output', dest='local_out', default='full_out_tmp',
                  help='Base directory for placing local files')
parser.add_option('-n', '--ncols', type='int', dest='ncols', default=0,
                  help='number of columns in the matrix')
parser.add_option('-s', '--schedule', dest='sched', default='100,100,100',
                  help='comma separated list of number of map tasks to use for'
                       + ' the three jobs')
parser.add_option('-H', '--hadoop', dest='hadoop', default='',
                  help='name of hadoop for Dumbo')

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

hadoop = options.hadoop


# Now run the MapReduce jobs
out1 = out + '_1'
cm.run_dumbo('full1.py', hadoop, ['-mat ' + in1, '-output ' + out1,
                                  '-nummaptasks %d' % sched[0],
                                  '-libjar feathers.jar'])

out2 = out + '_2'
cm.run_dumbo('full2.py', hadoop, ['-mat ' + out1 + '/R_*', '-output ' + out2,
                                  '-svd ' + str(svd_opt),
                                  '-nummaptasks %d' % sched[1],
                                  '-libjar feathers.jar'])

# Q2 file needs parsing before being distributed to phase 3
Q2_file = out_file('Q2.txt')

if os.path.exists(Q2_file):
  os.remove(Q2_file)

if os.path.exists(Q2_file + '.out'):
  os.remove(Q2_file + '.out')

cm.copy_from_hdfs(out2 + '/Q2', Q2_file)
cm.parse_seq_file(Q2_file)

in3 = out1 + '/Q_*'
cm.run_dumbo('full3.py', hadoop, ['-mat ' + in3, '-output ' + out + '_3',
                                  '-ncols ' + str(ncols),
                                  '-q2path ' + Q2_file + '.out',
                                  '-nummaptasks %d' % sched[2],
                                  '-libjar feathers.jar'])

if svd_opt == 2:
  small_U_file = out_file('U.txt')

  if os.path.exists(small_U_file):
    os.remove(small_U_file)
  if os.path.exists(small_U_file + '.out'):
    os.remove(small_U_file + '.out')

  cm.copy_from_hdfs(out2 + '/U', small_U_file)
  cm.parse_seq_file(small_U_file)

  # We need an addition TS matrix multiply to get the left singular vectors
  out4 = out + '_4'

  cm.run_dumbo ('TSMatMul.py', hadoop, ['-mat ' + out + '_3', '-output ' + out4,
                                        '-mpath ' + small_U_file + '.out',
                                        '-nummaptasks %d' % sched[2]])

cm.output('times: ' + str(cm.times))
