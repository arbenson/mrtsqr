#!/usr/bin/env python

"""
This is a script to run the TSQR with one step of iterative refinement to
compute Q.

See options:
     python run_tsqr_ir.py --help

Example usage:
     python run_tsqr_ir.py --input=A_600M_25.bseq --blocksize=5 \
     --hadoop=icme-hadoop1 --output=IR_TESTING --times_output=ir_times


Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014

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

def tsqr_arinv_iter(in1, out):
    out1 = out + '_qrr'
    script = 'tsqr.py'
    args = ['-mat ' + in1, '-blocksize ' + str(blocksize),
            '-output ' + out1, '-reduce_schedule %d,%d' % (sched[0], sched[1])]
    if use_cholesky != 0:
      script = 'CholeskyQR.py'
      args += ['-ncols %d' % use_cholesky]

    cm.run_dumbo(script, hadoop, args)

    R_file = out1 + '_R'
    if os.path.exists(R_file):
      os.remove(R_file)
    cm.copy_from_hdfs(out1, R_file)
    cm.parse_seq_file(R_file)

    out2 = out + '_Q'
    cm.run_dumbo('ARInv.py', hadoop, ['-mat ' + in1,
                                      '-blocksize ' + str(blocksize),          
                                      '-output ' + out2,
                                      '-matpath ' + R_file + '.out'])

tsqr_arinv_iter(in1, out)
tsqr_arinv_iter(out + '_Q', out + '_IR')

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times))
  f.write('\n')
  f.close
except:
  print str(cm.times)
