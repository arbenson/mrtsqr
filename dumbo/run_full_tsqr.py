#!/usr/bin/env python

"""
This is a script to run the Full TSQR algorithm with direct computation
of the matrix Q.

usage:

    python run_full_tsqr.py matrix_name [map_schedule] [output_name]

matrix_name is the name of the input file that is stored in the HDFS.

map_schedule is a string representation of a comma-separated list of length three.
The list is used to set Hadoop's nummaptasks option for each phase.  For example,
"50, 60, 40" will set nummaptasks to 50 for the first phase, 60 for the second
phase, and 40 for the third phase.  The algorithm is designed to scale for map
tasks, so each part of the schedule should be as large as possible.  If this
argument is not provided, then "100, 100, 100" is used.  Note that the actual
number of map tasks used may differ from the option provided to Hadoop.

output_name is used to derive the name of the outpute directories.
The outputs of the three jobs will be stored in output_name_1,
output_name_2, and output_name_3.

This script relies on the script q2parse.sh, which is used for a preliminary
parsing of the output of phase 2.  q2parse.sh was designed to handle the
output on NERSC's Magellan cluster.  It has not been tested on other
Hadoop clusters.

Austin R. Benson arbenson@gmail.com
Copyright (c) 2012
"""

import os
import sys

def sched_usage(arg):
    
    print "map_schedule needs "
    sys.exit(-1)

try:
    in1 = sys.argv[1]
except:
    usage()

try:
    sched = sys.argv[2]
    sched = [int(s) for s in sched.split(',')]
    sched[2]
    print "schedule: " + str(sched[0:3])
except:
    sched = [100,100,100]
    print "schedule: " + str(sched)    

try:
    out = sys.argv[3]
except:
    out = "%s_FULL" % (in1)


split = "-"*60

out1 = "%s_1" % (out)
cmd1 = "dumbo start full1.py -mat %s -output %s -use_system_numpy -nummaptasks %d \
        -hadoop nersc -libjar feathers.jar" % (in1, out1, sched[0])
print "running first phase..."
print "(command is: %s)" % (cmd1)
print split
os.system(cmd1)

in2 = "%s/R_*" % (out1)
out2 = "%s_2" % (out)
cmd2 = "dumbo start full2.py -mat %s -output %s -use_system_numpy -nummaptasks %d \
        -hadoop nersc -libjar feathers.jar" % (in2, out2, sched[1])

print "running second phase..."
print "(command is: %s)" % (cmd2)
print split
os.system(cmd2)

# Q2 file needs parsing before being distributed to phase 3
Q2_file = "%s_Q2.txt" % (out2)
cat_cmd = "dumbo cat %s/Q2 -hadoop nersc > %s" % (out2, Q2_file)
os.system(cat_cmd)

parse_cmd = "sh q2parse.sh %s" % (Q2_file)
os.system(parse_cmd)

in3 = "%s/Q_*" % (out1)
out3 = "%s_3" % (out)
cmd3 = "dumbo start full3.py -mat %s -output %s -q2path %s -use_system_numpy \
        -nummaptasks %d -hadoop nersc -libjar feathers.jar" % (in3, out3, Q2_file, sched[2])

print "running third phase..."
print "(command is: %s)" % (cmd3)
print split
os.system(cmd3)

rm_cmd = "rm -rf %s" % (Q2_file)
os.system(rm_cmd)
