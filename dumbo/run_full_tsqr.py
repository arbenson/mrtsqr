#!/usr/bin/env python

"""
This is a script to run the Full TSQR algorithm with direct computation
of the matrix Q.

usage:

    python run_full_tsqr.py matrix_name [output_name]

matrix_name is the name of the input file that is stored in the HDFS.
output_name is used to derive the name of the outpute directories.
The outputs of the three jobs will be stored in output_name_1,
output_name_2, and output_name_3.

This script relies on the script parse.sh, which is used for a preliminary
parsing of the output of phase 2.  parse.sh was designed to handle the
output on NERSC's Magellan cluster.  It has not been tested on other
Hadoop clusters.

Austin R. Benson arbenson@gmail.com
Copyright (c) 2012
"""

import os
import sys

def usage():
    print "usage: python run_full_tsqr.py matrix_name [output_name]"
    sys.exit(-1)

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
cmd1 = "dumbo start full1.py -mat %s -output %s -use_system_numpy \
        -hadoop nersc -libjar feathers.jar" % (in1, out1)
os.system(cmd1)

in2 = "%s/R_*" % (out1)
out2 = "%s_2" % (out1)
cmd2 = "dumbo start full2.py -mat %s -output %s -use_system_numpy \
        -hadoop nersc -libjar feathers.jar" % (in2, out2)

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
cmd3 = "dumbo start full3.py -mat %s -output %s -q2path %s -use_system_numpy \
        -hadoop nersc -libjar feathers.jar" % (in3, out3, Q2_file)

print "running third phase..."
print split
os.system(cmd3)
