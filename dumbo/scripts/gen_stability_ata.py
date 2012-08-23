#!/usr/bin/env python

import util

cm = util.CommandManager()
for j in xrange(3):
    for i in xrange(17):
        cmd = 'dumbo start AtA.py '
        if j == 0:
            cmd += '-mat Stability_caqr_%d_Q ' % i
        elif j == 1:
            cmd += '-mat Stability_caqr_%d_IR_Q ' % i
        elif j == 2:
            cmd += '-mat Stability_full_%d_3 ' % i
        cmd += '-reduce_schedule 1 '
        cmd += '-hadoop icme-hadoop1 '
        cm.exec_cmd(cmd)
