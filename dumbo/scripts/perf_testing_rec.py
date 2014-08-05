"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

import util, sys
cm = util.CommandManager()

matrices = [('A_500M_50.bseq', 2000, 10), ('A_4B_4.bseq', 2000, 400),
            ('A_2500M_10.bseq', 2600, 200), ('A_150M_100.bseq', 1400, 10),
            ('A_800M_10.bseq', 800, 200), ('A_600M_25.bseq', 1600, 150),
            ('A_15M_1500.bseq', 800, 3), ('A_50M_200.bseq', 920, 3)]

dirtsqr = True
if dirtsqr:
  for i, matrix in enumerate(matrices):
    if i != 7: continue
    cmd = 'python run_rec_dirtsqr.py '
    cmd += '--input=%s ' % matrix[0]
    ncols = matrix[0].strip('.bseq').split('_')[2]
    cmd += '--ncols=%s ' % ncols
    cmd += '--schedule=%d,%d,%d ' % (matrix[1], matrix[1], matrix[1])
    cmd += '--times_output=dirtsqr_recursive_perf '
    cmd += '--output=DIRTSQR_PERF_TESTING '
    cmd += '--hadoop=icme-hadoop1 '
    cm.exec_cmd(cmd)
