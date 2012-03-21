#!/usr/bin/env python

"""
Script to run the full MRTSQR algorithm.

CURRENTLY ONLY RUNS PHASES 1 AND 2
"""

import os
import sys

def usage():
    print "usage: python run_full_tsqr.py matrix_name [output_name]"

try:
    in1 = sys.argv[1]
except:
    usage()

try:
    out1 = sys.argv[2]
except:
    out1 = "%s_FULL" % (in1)

split = "-"*60

print "running first phase..."
print split
cmd1 = "dumbo start full1.py -mat %s -output %s -use_system_numpy -hadoop nersc -libjar feathers.jar" %(in1, out1)
os.system(cmd1)

in2 = "%s/R_*" % (out1)
out2 = "%s_2" % (out1)
cmd2 = "dumbo start full2.py -mat %s -output %s -use_system_numpy -hadoop nersc -libjar feathers.jar" %(in2, out2)

print "running second phase..."
print split
os.system(cmd2)

# Q2 file needs parsing before being distributed to phase 3
Q2_file = "%s_Q2.txt" % (out1)
cat_cmd = "dumbo cat %s/Q2 -hadoop nersc > %s" % (out2, Q2_file)





