#!/usr/bin/env python
import numpy

"""
This is a script to run the TSQR with Pseudo Iterative refinement to
compute Q.

See options:
     python run_tsqr_pir.py --help

Example usage:

Austin R. Benson     arbenson@stanford.edu
David F. Gleich
Copyright (c) 2012

This script is designed to run on ICME's MapReduce cluster, icme-hadoop1.
"""

from optparse import OptionParser
import os
import subprocess
import sys
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
parser.add_option('-s', '--schedule', dest='sched', default='40,1',
                  help='comma separated list of number of reduce tasks')
parser.add_option('-H', '--hadoop', dest='hadoop', default='',
                  help='name of hadoop for Dumbo')
parser.add_option('-m', '--nummaptasks', dest='nummaptasks', default=100,
                  help='name of hadoop for Dumbo')
parser.add_option('-b', '--blocksize', dest='blocksize', default=3,
                  help='blocksize')
parser.add_option('-c', '--use_cholesky', dest='use_cholesky', default=0,
                  help='provide number of columns for cholesky')
parser.add_option('-q', '--quiet', action='store_false', dest='verbose',
                  default=True, help='turn off some statement printing')
parser.add_option('-t', '--times_output', dest='times_out', default='times',
                  help='file for storing command times')

(options, args) = parser.parse_args()
cm = util.CommandManager(verbose=options.verbose)
use_cholesky = int(options.use_cholesky)

times_out = options.times_out

# Store options in the appropriate variables
in1 = options.input
if in1 == '':
  cm.error('no input matrix provided, use --input')

out = options.out
if out == '':
  # TODO(arbenson): make sure in1 is clean
  out = in1 + '-qir'

sched = options.sched
try:
  sched = [int(s) for s in sched.split(',')]
  sched[1]
except:
  cm.error('invalid schedule provided')

nummaptasks = int(options.nummaptasks)
blocksize = options.blocksize

hadoop = options.hadoop

use_cholesky = int(options.use_cholesky)

# Sample step
out1 = out + '_pir_R1'
script = 'tsqr.py'
args = ['-mat ' + in1 + '/part-00000', '-blocksize ' + str(blocksize),
        '-output ' + out1, '-reduce_schedule 1,1']
if use_cholesky != 0:
  script = 'CholeskyQR.py'
  args += ['-ncols %d' % use_cholesky]
cm.run_dumbo(script, hadoop, args)

# Copy sample R locally
pir_R1 = out1
if os.path.exists(pir_R1):
  os.remove(pir_R1)
cm.copy_from_hdfs(out1, pir_R1)
cm.parse_seq_file(pir_R1)

# Run TSQR with the sample as a premultiplier
out2 = out + '_pir_R2'
args = ['-mat ' + in1, '-blocksize ' + str(blocksize), '-mpath ' + pir_R1 + '.out',
        '-output ' + out2, '-reduce_schedule %d,%d' % (sched[0], sched[1])]
if use_cholesky != 0:
  args += ['-ncols %d' % use_cholesky]
cm.run_dumbo(script, hadoop, args)

# Copy sample R locally
pir_R2 = out2
if os.path.exists(pir_R2):
  os.remove(pir_R2)
cm.copy_from_hdfs(out2, pir_R2)
cm.parse_seq_file(pir_R2)

out3 = out + '_pir_Q'
cm.run_dumbo('ARInv2.py', hadoop, ['-mat ' + in1,
                                   '-blocksize ' + str(blocksize),
                                   '-output ' + out3,
                                   '-matpath ' + pir_R1 + '.out',
                                   '-matpath2 ' + pir_R2 + '.out'])

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times))
  f.write('\n')
  f.close
except:
  print str(cm.times)
