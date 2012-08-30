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
from optparse import OptionParser

cm = util.CommandManager(True)
hadoop = 'icme-hadoop1'
times_out = 'house_perf_numbers'

inp = 'HHQR_test_matrix.mseq'
out = 'HH_TESTING'

num_steps = 3
for step in xrange(num_steps):
  out1 = out + '_1'
  if step != 0:
    args = ['-mat ' + out3 + '/A_matrix',
            '-info_file %s.out' % info_file, '-w_file %s.out' % w_file]
  else:
    args = ['-mat ' + inp]
  args += ['-output ' + out1, '-step ' + str(step), '-nummaptasks 10',
           '-libjar feathers.jar']
  cm.run_dumbo('HouseholderQR_1.py', hadoop, args)

  out2 = out + '_2'
  args = ['-mat ' + out1 + '/KV_output', '-output ' + out2,
          '-step ' + str(step), '-libjar feathers.jar']
  if step != 0:
    args += ['-info_file %s.out' % info_file]
  cm.run_dumbo('HouseholderQR_2.py', hadoop, args)


  info_file = 'part_2_info_STEP_' + str(step)
  cm.copy_from_hdfs(out2, info_file)
  cm.parse_seq_file(info_file)

  out3 = out + '_3'
  cm.run_dumbo('HouseholderQR_3.py', hadoop,
               ['-mat ' + out1 + '/A_matrix', '-output ' + out3, '-step ' + str(step),
                '-info_file %s.out' % info_file, '-libjar feathers.jar'])

  if step != num_steps - 1:
    out4 = out + '_4'
    cm.run_dumbo('HouseholderQR_4.py', hadoop,
                 ['-mat ' + out3 + '/KV_output', '-output ' + out4,
                  '-libjar feathers.jar'])

    w_file = 'part_4_info_STEP_' + str(step)
    cm.copy_from_hdfs(out4, w_file)
    cm.parse_seq_file(w_file)

out_final = out + '_FINAL'
cm.run_dumbo('HouseholderQR_1.py', hadoop,
             ['-mat ' + out3 + '/A_matrix', '-info_file %s.out' % info_file,
              '-w_file %s.out' % w_file, '-output ' + out_final,
              '-step ' + str(step), '-last_step 1', '-nummaptasks 10',
              '-libjar feathers.jar'])

try:
  f = open(times_out, 'a')
  f.write('times: ' + str(cm.times) + '\n')
  f.close
except:
  print str(cm.times)
