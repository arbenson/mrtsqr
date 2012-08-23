#!/usr/bin/env python

"""
This is a script to run the TSQR with one step of iterative refinement to
compute Q.

See options:
     python run_full_tsqr.py --help

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
  out = in1 + '-qir'

sched = options.sched
try:
  sched = [int(s) for s in sched.split(',')]
  sched[1]
except:
  cm.error('invalid schedule provided')

hadoop = options.hadoop


def tsqr_arinv_iter(in1, out):
    blocksize = 10
    
    out1 = out + '_qrr'
    cm.run_dumbo('tsqr.py', hadoop, ['-mat ' + in1,
                                     '-blocksize ' + str(blocksize),
                                     '-output ' + out1,
                                     '-reduce_schedule 20,1'])
    cm.output('running tsqr...')

    R_file = out1 + '_R'
    if os.path.exists(R_file):
      os.remove(R_file)
    cm.copy_from_hdfs(out1, R_file)
    cm.parse_seq_file(R_file)

    out2 = out + '_Q'
    cm.run_dumbo('ARInv.py', hadoop, ['-mat ' + in1,
                                      '-blocksize ' + str(blocksize),          
                                      '-output ' + out2,
                                      '-rpath ' + R_file + '.out'])

tsqr_arinv_iter(in1, out)
tsqr_arinv_iter(out + '_Q', out + '_IR')

cm.output('times: ' + str(cm.times))
