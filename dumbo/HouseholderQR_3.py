#!/usr/bin/env dumbo

"""
Householder algorithm driver for MapReduce (part 1)

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

#('picked_set: ', ['d32ee466f24b11e19082002590764929'])
#('alpha: ', 0.46301049163578767)
#('tau: ', 1.0046680219967319)
#('sigma: ', 0.01003504865849064)

def parse_extra_info(f):
  # TODO(arbenson): implement this, should return alpha, sigma, picked_set
  alpha = 0.46301049
  sigma = 0.010035
  picked_set = ['d32ee466f24b11e19082002590764929']
  return (alpha, sigma, picked_set)

def runner(job):
    step = gopts.getintkey('step')
    if step == -1:
      print "need step parameter!"
      sys.exit(-1)

    extra_info_path = gopts.getstrkey('extra_info_path')
    alpha, sigma, picked_set = parse_extra_info(extra_info_path)
   
    mapper = HouseholderQR.HouseholderMap3(alpha, sigma, step, picked_set)
    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks','0')])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, use_full=False, use_house=True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-HHQR-3%s'%(matname,matext))

    step = int(prog.getopt('step'))

    path = 'extra_info_path'
    path_opt = prog.delopt(path)
    if not path_opt:
      return "'%s' not specified" % path
    gopts.getstrkey(path, path_opt)

    gopts.getintkey('step', -1)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
