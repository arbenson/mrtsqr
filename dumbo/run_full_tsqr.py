#!/usr/bin/env python

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
os.system(cat_cmd)

parse_cmd = "sh parse.sh %s" % (Q2_file)
os.system(parse_cmd)

in3 = "%s/Q_*" % (out1)
out3 = "%s_3" % (out1)
cmd3 = "dumbo start full3.py -mat %s -output %s -q2path %s -use_system_numpy -hadoop nersc -libjar feathers.jar" % (in3, out3, Q2_file)

print "running third phase..."
print split
os.system(cmd3)
