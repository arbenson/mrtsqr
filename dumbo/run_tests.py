#!/usr/bin/env python

import numpy
import os
import subprocess
import sys
import time
import util

out_dir = 'tests-out'
try:
  os.mkdir(out_dir)
except:
  pass

cm = util.CommandManager()

# deal with annoying new lines
def format_row(row):
  val_str = ''
  for val in row:
    val_str += str(val)
    val_str += ' '
  return val_str[:-1]

def scaled_ones(nrows, ncols, scale, file):
  f = open(file, 'w')
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
                                                     '-nummaptasks 10'])

def txt_to_bseq(inp, outp):
  cm.copy_to_hdfs(inp, inp)
  cm.run_dumbo('matrix2seqfile.py', 'icme-hadoop1', ['-input ' + inp,
                                                     '-output ' + outp,
                                                     'use_tb_str',
                                                     '-nummaptasks 10'])

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
  ts_mat = 'tsmatmul-1000-8'
  # TODO(arbenson): cleaner way to handle out_dir
  ts_mat_out = '%s/%s' % (out_dir, ts_mat)
  small_mat = 'tsmatmul-8-8'
  small_mat_out = '%s/%s' % (out_dir, small_mat)
  result = 'TSMatMul_test'
  result_out = '%s/%s' % (out_dir, result)

  if not os.path.exists(ts_mat_out):
    scaled_ones(1000, 8, 4, ts_mat_out)
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    txt_to_mseq(ts_mat_out, ts_mat + '.mseq')
    
  if not os.path.exists(small_mat_out):
    scaled_identity(8, 2, small_mat_out)

  cm.run_dumbo('TSMatMul.py', 'icme-hadoop1', ['-mat ' + ts_mat + '.mseq',
                                               '-output ' + result_out,
                                               '-mpath ' + small_mat_out,
                                               '-nummaptasks 1'])

  # we should only have one output file
  cm.copy_from_hdfs(result_out, result_out + '.mseq')
  cm.parse_seq_file(result_out + '.mseq', result_out + '.txt')
  good_rows = check_rows(result_out + '.txt', [8]*8)
  return good_rows == 1000

def ARInv_test():
  # TODO(arbenson): finish this test
  return False

def tsqr_test():
  # TODO(arbenson): finish this test
  return False

def CholeskyQR_test():
  # TODO(arbenson): finish this test
  return False

def BtA_test():
  # TODO(arbenson): finish this test
  return False

def full_tsqr_test():
  # TODO(arbenson): finish this test
  return False
  mat = 'Q-2000-20'
  mat_out = '%s/%s' % (out_dir, mat)

  if not os.path.exists(mat_out):
    # TODO(arbenson): Check HDFS instead of just assuming that the local
    # and HDFS copies are consistent
    orthogonal(2000, 20, mat_out)
    txt_to_mseq(mat_out, mat + '.mseq')


  out = 'full_tsqr_test_out'

  # compute full TSQR with SVD
  cmd = 'python run_full_tsqr.py'
  cmd += ' --input=' + mat + '.mseq'
  cmd += ' --output=' + out
  cmd += ' --ncols=20'
  cmd += ' --schedule=12,12,12'
  cmd += ' --svd=2'

  # copy Sigma, V^t locally

  # make sure that Sigma, V^t are diagonal ones

def tsqr_ir_test():
  return False

tests = {'TSMatMul':        TSMatMul_test,
         'ARInv':           ARInv_test,
         'tsqr':            tsqr_test,
         'CholeskyQR':      CholeskyQR_test,
         'BtA':             BtA_test,
         'full_tsqr':       full_tsqr_test,
         'tsqr_ir':         tsqr_ir_test}

failures = []
args = sys.argv[1:]

if len(args) > 0 and args[0] == 'all':
  args = tests.keys()

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
