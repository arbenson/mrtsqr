"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

import util
cm = util.CommandManager()

for i, scale in enumerate([10 ** i for i in xrange(17)]):
  cmd = 'dumbo start parse_and_change.py '
  cmd += '-mat Simple_1M.tmat '
  cmd += '-scale %d ' % scale
  cmd += '-output Stability_test_%d.mseq ' % i
  cmd += '-nummaptasks 40 '
  cmd += '-hadoop icme-hadoop1 '
  cm.exec_cmd(cmd)
