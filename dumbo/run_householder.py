#!/usr/bin/env python

"""
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
import math
import HouseholderQR
from optparse import OptionParser

cm = util.CommandManager(True)
hadoop = 'icme-hadoop1'
times_out = 'house_perf_numbers'

inp = 'HHQR_test_matrix.mseq'
inp = 'Simple_1k_10.bseq'
out = 'HH_TESTING'

num_steps = 3
num_steps = 10

def gen_next_info_file(input_file, info_file, out_file):
  picked = None
  alpha = None
  norm = 0.0
  picked_set = []
  if info_file is not None:
    picked_set, _, _, _ = HouseholderQR.parse_info(info_file)
  
  for line in open(input_file, 'r'):
    ind1 = line.find('(')
    ind2 = line.find(',')
    key = line[ind1 + 1:ind2]
    if key[0] == '\'': key = key[1:-1]
    key = str(key)
    val = line[ind2:]
    ind1 = val.find('[')
    ind2 = val.find(']')
    val = val[ind1 + 1:ind2].split(',')
    value = float(val[0])
    if len(val) == 2:
      if picked is None:
        picked = key
        alpha = value
      norm += value * value
    else:
      norm += value


  out = open(out_file, 'w')  
  beta = math.sqrt(norm)
  out.write('(beta) %d\n' % beta)

  beta = float(-1.0) * math.copysign(beta, alpha)
  tau = (beta - alpha) / beta
  sigma = float(1.0) / (alpha - beta)
  alpha = beta
  picked_set.append(picked)
  
  out.write('(picked_set) %s\n' % picked_set)
  out.write('(alpha) %f\n' % alpha)
  out.write('(tau) %f\n' % tau)
  out.write('(sigma) %f\n' % sigma)

for step in xrange(num_steps):
  out1 = out + '_1_' + str(step)
  if step != 0:
    args = ['-mat ' + out + '_1_' + str(step - 1) + '/A_matrix',
            '-info_file ' + info_file, '-w_file %s.out' % w_file]
    if step > 1:
      cm.exec_cmd('hadoop fs -rmr %s_1_%d' % (out, step - 2))
  else:
    args = ['-mat ' + inp]

  args += ['-output ' + out1, '-step ' + str(step), '-libjar feathers.jar']
  cm.run_dumbo('HouseholderQR_1.py', hadoop, args)

  input_file = 'part_2_input_file_%d' % step
  cmd = 'dumbo cat %s/KV_output/part-* -hadoop icme-hadoop1 > %s' % (out1, input_file)
  cm.exec_cmd(cmd)
  args = [input_file, None, 'part_2_info_STEP_' + str(step) + '.out']
  if step != 0:
    args[1] = info_file
  gen_next_info_file(*args)
  info_file = args[2]

  out2 = out + '_2'
  cm.run_dumbo('HouseholderQR_2.py', hadoop,
               ['-mat ' + out1 + '/A_matrix', '-output ' + out2,
                '-step ' + str(step), '-info_file ' + info_file,
                '-libjar feathers.jar'])

  w_file = 'part_2_w_info_STEP_' + str(step)
  cm.copy_from_hdfs(out2, w_file)
  cm.parse_seq_file(w_file)

out_final = out + '_FINAL'
cm.run_dumbo('HouseholderQR_1.py', hadoop,
             ['-mat ' + out1 + '/A_matrix', '-info_file ' + info_file,
              '-w_file %s.out' % w_file, '-output ' + out_final,
              '-step ' + str(step + 1), '-last_step 1', '-libjar feathers.jar'])

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times) + '\n')
  f.close
except:
  print str(cm.times)
