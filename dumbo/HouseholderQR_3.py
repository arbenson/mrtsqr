#!/usr/bin/env dumbo

"""
Householder algorithm driver for MapReduce (part 3)

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import mrmc
import dumbo
import util
import sys
import os
import HouseholderQR

# create the global options structure
gopts = util.GlobalOptions()

def runner(job):
    step = gopts.getintkey('step')
    if step == -1:
      print "need step parameter!"
      sys.exit(-1)

    info_file = gopts.getstrkey('info_file')
   
    mapper = HouseholderQR.HouseholderMap3(step, info_file)
    reducer = HouseholderQR.HouseholderRed3()
    job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks', '1')])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, use_dirtsqr=False, use_house=True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-HHQR-3%s'%(matname,matext))

    step = int(prog.getopt('step'))

    path = 'info_file'
    path_opt = prog.delopt(path)
    if not path_opt:
      return "'%s' not specified" % path
    prog.addopt('file', os.path.join(os.path.dirname(__file__), path_opt))
    gopts.getstrkey(path, path_opt)

    gopts.getintkey('step', -1)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
