#!/usr/bin/env python

import numpy
import os

out_dir = 'tests-out'
try:
  os.mkdir(out_dir)
except:
  pass

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

# deal with annoying new lines
def format_row(row):
  val_str = ''
  for val in row:
    val_str += str(val)
    val_str += ' '
  return val_str[:-1]

def scaled_ones(nrows, ncols, scale, name):
  f = open(out_dir + '/' + name, 'w')
  A = numpy.ones((nrows, ncols))*scale
  for row in A:
    f.write(format_row(row) + '\n')
  f.close()

def scaled_identity(nrows, scale, name):
  f = open(out_dir + '/' + name, 'w')
  A = numpy.identity(nrows)*scale
  for row in A:
    f.write(format_row(row) + '\n')
  f.close()

def txt_to_mseq(name):
  cmd = 'hadoop fs -copyFromLocal %s %s' (name, name)


def txt_to_bseq(name):

def TSMatMul_test():
  name = 'tsmatmul-1000-16'
  scaled_ones(1000, 16, 4, 'tsmatmul-1000-16')
  scaled_identity(16, 2, 'tsmatmul-16-16')


