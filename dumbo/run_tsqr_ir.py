#!/usr/bin/env python

"""
This is a script to run the TSQR with one step of iterative refinement to
compute Q.

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
David F. Gleich
Copyright (c) 2012
"""

import os
import sys
import time

in1 = sys.argv[1]

try:
    sched = sys.argv[2]
    sched = [int(s) for s in sched.split(',')]
    sched[3]
    print "schedule: " + str(sched[0:4])
except:
    sched = [100,100,100,100]
    print "schedule: " + str(sched)    

try:
    out = sys.argv[3]
except:
    out = "%s-qir" % (in1)

times = []
split = "-"*60

def tsqr_arinv_pipeline(in1, out):
    out1 = "%s_qrr" % (out)
    cmd1 = "dumbo start tsqr.py -mat %s -blocksize 5 -output %s -use_system_numpy -nummaptasks %d \
    -reduce_schedule 138,1 -hadoop nersc" % (in1, out1, sched[0])
    print "running tsqr..."
    print "(command is: %s)" % (cmd1)
    print split

    t0 = time.time()
    os.system(cmd1)
    times.append(time.time() - t0)

    # parsing
    R_file = "%s_R" % out1
    cat_cmd = "dumbo cat %s/part-00000 -hadoop nersc > %s" % (out1, R_file)

    t0 = time.time()
    os.system(cat_cmd)
    times.append(time.time() - t0)

    parse_cmd = "sh q2parse.sh %s" % (R_file)
    os.system(parse_cmd)

    out2 = "%s_Q" % (out)
    cmd2 = "dumbo start ARInv.py -mat %s -output %s -rpath %s -use_system_numpy -nummaptasks %d \
    -hadoop nersc" % (in1, out2, R_file, sched[1])

    print "running arinv..."
    print "(command is: %s)" % (cmd2)
    print split

    t0 = time.time()
    os.system(cmd2)
    times.append(time.time() - t0)

tsqr_arinv_pipeline(in1, out)
tsqr_arinv_pipeline("%s_Q" % out, "%s_IR" % out)

print split
print "times: %s" % (str(times))
