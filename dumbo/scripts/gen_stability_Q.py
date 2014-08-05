"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

import util
cm = util.CommandManager()

for i, scale in enumerate([10 ** i for i in xrange(17)])
  cmd = 'python run_tsqr_ir.py '
  cmd += '--input=Stability_test_%d.mseq ' % i
  cmd += '--output=Stability_caqr_%d ' % i
  cmd += '--hadoop=icme-hadoop1 '
  cm.exec_cmd(cmd)

  cmd = 'python run_full_tsqr.py '
  cmd += '--input=Stability_test_%d.mseq ' % i
  cmd += '--output=Stability_full_%d ' % i
  cmd += '--hadoop=icme-hadoop1 '
  cmd += '--ncols=10 '
  cmd += '--schedule=20,20,20'
  cm.exec_cmd(cmd)
