#!/usr/bin/env python

import mrmc
import numpy
import os
import sys

verbose = True
times = []
split = '-'*60

out_dir = 'tests-out'
try:
  os.mkdir(out_dir)
except:
  pass

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
def parse_seq_file(input, output):
  parse_cmd = 'python hyy-python-hadoop/examples/SequenceFileReader.py %s > %s' % (input, output)
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

def txt_to_mseq(input, output):
  run_dumbo('matrix2seqfile.py', 'icme-hadoop1', ['-mat ' + input,
                                                  '-output ' + output,
                                                  '-nummaptasks 10'])

def txt_to_bseq(input, output):
  run_dumbo('matrix2seqfile.py', 'icme-hadoop1', ['-mat ' + input,
                                                  '-output ' + output,
                                                  'use_tb_str',
                                                  '-nummaptasks 10'])

def check_rows(file, num_rows, comp_row):
  f = open(file)
  good_rows = 0
  for row in f:
    ind = line.rfind(')')
    line = line[ind+3:]
    line = line.lstrip('[').rstrip().rstrip(']')
    try:
      line2 = line.split(',')
      line2 = [int(v) for v in line2]
    except:
      line2 = line.split()
      line2 = [int(v) for v in line2]
    
    if len(line2) != len(comp_row):
      continue
    else:
      good = True
      for i, val in enumerate(comp_row):
        if val != comp_row[i]:
          good = False
          break
      if good:
        good_rows += 1

  f.close()
  return good_rows

def print_result(test, success):
  print '-'*40
  print 'TEST: ' + test
  print '    ' + 'SUCCESS' if success else 'FAILURE'
  print '-'*40  

def TSMatMul_test():
  ts_mat = 'tsmatmul-1000-16'
  # TODO(arbenson): cleaner way to handle out_dir
  ts_mat_out = '%s/%s' % (out_dir, ts_mat)
  small_mat = 'tsmatmul-16-16'
  small_mat_out = '%s/%s' % (out_dir, small_mat)
  result = 'TSMatMul_test'
  result_out = '%s/%s' % (out_dir, result)

  scaled_ones(1000, 16, 4, name)
  txt_to_mseq(ts_mat_out, ts_mat + '.mseq')
  scaled_identity(16, 2, 'tsmatmul-16-16')
  run_dumbo('TSMatMul.py', 'icme-hadoop1', ['-mat ' + ts_mat + '.mseq',
                                            '-output ' + test_out,
                                            '-mpath ' + small_mat_out,
                                            '-nummaptasks 1'])

  # we should only have one output file
  copy_cmd = 'hadoop fs -copyToLocal %s/part-00000 %s' % (result, result_out + '.mseq')
  parse_seqfile(result_out, result_out + '.txt')
  good_rows = row_by_row(result_out + '.txt', 1000, [8]*16)
  return good_rows == 1000

tests = {'TSMatMul_test': TSMatMul_test}

failures = []
for arg in sys.argv[1:]:
  try:
    result = tests[arg]()
    print_result(arg, result)
    if not result:
      failures.append(arg)
  except:
    print 'unrecognized test: ' + arg

if len(failures) == 0:
  print 'ALL TESTS PASSED'
else:
  print 'FAILED %d TESTS:' % len(failures)
  for failed_test in failures:
    print '     ' + failed_test
