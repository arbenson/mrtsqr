#!/usr/bin/env python

import numpy
import os
import shutil
import subprocess
import sys
import time
import util
import math

# Initialization
out_dir = 'tests-out'
try:
  os.mkdir(out_dir)
except:
  pass
out_file = lambda f: out_dir + '/' + f
cm = util.CommandManager()

# deal with annoying new lines
def format_row(row):
  val_str = ''
  for val in row:
    val_str += str(val)
    val_str += ' '
  return val_str[:-1]

def scaled_ones(nrows, ncols, scale, f):
  f = open(f, 'w')
  A = numpy.ones((nrows, ncols))*scale
  for row in A:
    f.write(format_row(row) + '\n')
  f.close()

def scaled_identity(nrows, scale, f):
  f = open(f, 'w')
  A = numpy.identity(nrows)*scale
  for row in A:
    f.write(format_row(row) + '\n')
  f.close()

def orthogonal(nrows, ncols, f):
  f = open(f, 'w')
  Q = numpy.linalg.qr(numpy.random.randn(nrows, ncols))[0]
  for row in Q:
    f.write(format_row(row) + '\n')
  f.close()

def txt_to_mseq(inp, outp):
  cm.copy_to_hdfs(inp, inp)
  cm.run_dumbo('matrix2seqfile.py', 'icme-hadoop1', ['-input ' + inp,
                                                     '-output ' + outp,
                                                     '-nummaptasks 1'])

def txt_to_bseq(inp, outp):
  cm.copy_to_hdfs(inp, inp)
  cm.run_dumbo('matrix2seqfile.py', 'icme-hadoop1', ['-input ' + inp,
                                                     '-output ' + outp,
                                                     'use_tb_str',
                                                     '-nummaptasks 1'])

def clean():
  if os.path.exists(out_dir):
    shutil.rmtree(out_dir)
  cm.exec_cmd('hadoop fs -rmr ' + out_dir)

def check_diag_ones(f, nrows):
  abs_int = lambda x: int(round(math.fabs(x)))
  i = 0
  for i, row in enumerate(util.parse_matrix_txt(f)):
    print row
    if len(row) != nrows:
      print 'Expected row of length %d but got row of length %d' % (
          nrows, len(row))
      return False
    for j in xrange(nrows):
      if j == i and abs_int(row[j]) != 1:
        print 'Expected absolute value of 1 for entry (%d, %d) of value %d' % (
            i, j, abs_int(row[j]))
        return False
      elif j != i and abs_int(row[j]) != 0:
        print 'Expected absolute value of 0 for entry (%d, %d) of value %d' % (
            i, j, abs_int(row[j]))
        return False
  if i + 1 != nrows:
    print 'Expected %d rows but only read %d.' % (nrows, i)
    return False
  return True

def check_rows(f, comp_row):
  good_rows = 0
  for row in util.parse_matrix_txt(f):
    if len(row) != len(comp_row):
      continue
    good = True
    for i, val in enumerate(row):
      if int(val) != int(comp_row[i]):
        good = False
        break
    if good:
      good_rows += 1
  return good_rows

def print_result(test, success, output=None):
  print '-'*40
  print 'TEST: ' + test
  if success:
    print '    ***SUCCESS***'
  else:
    print '    ***FAILURE***'
  if output is not None:
    print output
  print '-'*40  

def TSMatMul_test():
  nrows = 1000
  ncols = 8
  ts_mat = 'tsmatmul-%d-%d' % (nrows, ncols)
  ts_mat_out = out_file(ts_mat)
  ts_mat_out_mseq = ts_mat_out + '.mseq'
  small_mat = 'tsmatmul-%d-%d' % (ncols, ncols)
  small_mat_out = out_file(small_mat)
  result = 'TSMatMul_test'
  result_out = out_file(result)
  result_out_mseq = result_out + '.mseq'
  result_out_txt = result_out + '.txt'

  if not os.path.exists(ts_mat_out):
    scaled_ones(nrows, ncols, 4, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat_out_mseq)
    
  if not os.path.exists(small_mat_out):
    scaled_identity(ncols, 2, small_mat_out)

  cm.run_dumbo('TSMatMul.py', 'icme-hadoop1', ['-mat ' + ts_mat_out_mseq,
                                               '-output ' + result_out,
                                               '-mpath ' + small_mat_out,
                                               '-nummaptasks 1'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out_mseq)
  cm.parse_seq_file(result_out_mseq, result_out_txt)
  good_rows = check_rows(result_out_txt, [8]*ncols)
  return good_rows == nrows

def ARInv_test():
  nrows = 2000
  ncols = 20
  ts_mat = 'arinv-%d-%d' % (nrows, ncols)
  ts_mat_out = out_file(ts_mat)
  ts_mat_out_mseq = ts_mat_out + '.mseq'
  small_mat = 'arinv-%d-%d' % (ncols, ncols)
  small_mat_out = out_file(small_mat)
  result = 'ARInv_test'
  result_out = out_file(result)
  result_out_mseq = result_out + '.mseq'
  result_out_txt = result_out + '.txt'

  if not os.path.exists(ts_mat_out):
    scaled_ones(nrows, ncols, 16, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat_out_mseq)
    
  if not os.path.exists(small_mat_out):
    scaled_identity(ncols, 2, small_mat_out)

  cm.run_dumbo('ARInv.py', 'icme-hadoop1', ['-mat ' + ts_mat_out_mseq,
                                            '-output ' + result_out,
                                            '-rpath ' + small_mat_out,
                                            '-nummaptasks 1'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out_mseq)
  cm.parse_seq_file(result_out_mseq, result_out_txt)
  good_rows = check_rows(result_out_txt, [8]*ncols)
  return good_rows == nrows

def tsqr_test():
  nrows = 400
  ncols = 4
  ts_mat = 'tsqr-%d-%d' % (nrows, ncols)
  ts_mat_out = out_file(ts_mat)
  ts_mat_out_mseq = ts_mat_out + '.mseq'
  result = 'tsqr_test'
  result_out = out_file(result)
  result_out_mseq = result_out + '.mseq'
  result_out_txt = result_out + '.txt'

  if not os.path.exists(ts_mat_out):
    orthogonal(nrows, ncols, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat_out_mseq)

  cm.run_dumbo('tsqr.py', 'icme-hadoop1', ['-mat ' + ts_mat_out_mseq,
                                           '-output ' + result_out,
                                           '-blocksize 10',
                                           '-reduce_schedule 2,1',
                                           '-nummaptasks 4'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out_mseq)
  cm.parse_seq_file(result_out_mseq, result_out_txt)
  return check_diag_ones(result_out_txt, ncols)

def CholeskyQR_test():
  nrows = 800
  ncols = 6
  ts_mat = 'chol-%d-%d' % (nrows, ncols)
  ts_mat_out = out_file(ts_mat)
  ts_mat_out_mseq = ts_mat_out + '.mseq'
  result = 'chol_test'
  result_out = out_file(result)
  result_out_mseq = result_out + '.mseq'
  result_out_txt = result_out + '.txt'

  if not os.path.exists(ts_mat_out):
    orthogonal(nrows, ncols, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat_out_mseq)

  cm.run_dumbo('CholeskyQR.py', 'icme-hadoop1', ['-mat ' + ts_mat_out_mseq,
                                                 '-output ' + result_out,
                                                 '-blocksize 10',
                                                 '-ncols %d' % ncols,
                                                 '-reduce_schedule 2,1',
                                                 '-nummaptasks 4'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out_mseq)
  cm.parse_seq_file(result_out_mseq, result_out_txt)
  return check_diag_ones(result_out_txt, ncols)

def BtA_test():
  nrows = 800
  ncols = 6
  ts_mat = 'bta-B_matrix-%d-%d' % (nrows, ncols)
  ts_mat_out = out_file(ts_mat)
  ts_mat_out_mseq = ts_mat_out + '.mseq'
  ts_mat_copy = 'bta-A_matrix-%d-%d' % (nrows, ncols)
  ts_mat_copy_out_mseq = out_file(ts_mat_copy) + '.mseq'

  result = 'bta_test'
  result_out = out_file(result)
  result_out_mseq = result_out + '.mseq'
  result_out_txt = result_out + '.txt'

  if not os.path.exists(ts_mat_out):
    orthogonal(nrows, ncols, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat_out_mseq)
    cm.exec_cmd('hadoop fs -cp %s %s' % (ts_mat_out_mseq, ts_mat_copy_out_mseq))

  cm.run_dumbo('BtA.py', 'icme-hadoop1', ['-matB ' + ts_mat_out_mseq,
                                          '-matA ' + ts_mat_copy_out_mseq,
                                          '-output ' + result_out,
                                          '-B_id ' + 'B_matrix',
                                          '-blocksize 10',
                                          '-reduce_schedule 2',
                                          '-nummaptasks 4'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out_mseq)
  cm.parse_seq_file(result_out_mseq, result_out_txt)
  return check_diag_ones(result_out_txt, ncols)

# TODO(arbenson): Add tests for run_dirtsqr.py and run_tsqr_ir.py.
tests = {'TSMatMul':        TSMatMul_test,
         'ARInv':           ARInv_test,
         'tsqr':            tsqr_test,
         'CholeskyQR':      CholeskyQR_test,
         'BtA':             BtA_test}

failures = []
args = sys.argv[1:]

if len(args) == 0:
  print 'Usage: python run_tests.py [tests]'
  sys.exit(-1)

if len(args) == 1 and args[0] == 'all':
  args = tests.keys()

if len(args) == 1 and args[0] == 'clean':
  clean()
  sys.exit(0)

for arg in args:
  if arg not in tests:
    print 'unrecognized test: ' + arg
    continue
  try:
    print 'RUNNING TEST: ' + arg
    result = tests[arg]()
  except:
    result = False
  print_result(arg, result)

  if not result:
    failures.append(arg)

if len(failures) == 0:
  print 'ALL TESTS PASSED'
else:
  print 'FAILED TESTS (%d total):' % len(failures)
  for failed_test in failures:
    print '     ' + failed_test
