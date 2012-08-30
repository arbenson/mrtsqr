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

#(1, -2.3658255930202912)
#(2, -1.0505953225529197)

#('picked_set: ', ['d32ee466f24b11e19082002590764929'])
#('alpha: ', 0.46301049163578767)
#('tau: ', 1.0046680219967319)
#('sigma: ', 0.01003504865849064)

def parse_extra_info(f):
  # TODO(arbenson): implement this, should return tau, picked_set
  #tau = None
  #picked_set = []

  tau = 1.00466
  picked_set = ['d32ee466f24b11e19082002590764929']
  return (tau, picked_set)

def parse_w(f):
  # TODO(arbenson): implement this, should set w
  #w = []
  w = [-2.3658255930202912, -1.0505953225529197]
  return w

def runner(job):
    step = gopts.getintkey('step')
    if step == -1:
      print "need step parameter!"
      sys.exit(-1)

    if step == 0:
      tau = 0
      picked_set =[]
      w = []
    else:
      extra_info_path = gopts.getstrkey('extra_info_path')
      w_path = gopts.getstrkey('w_path')
      tau, picked_set = parse_extra_info(extra_info_path)
      w = parse_w(w_path)
   
    mapper = HouseholderQR.HouseholderMap1(step, tau, picked_set, w)
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
        prog.addopt('output','%s-HHQR-1%s'%(matname,matext))

    step = int(prog.getopt('step'))

    if step != 0:
      for path in ['extra_info_path', 'w_path']:
        path_opt = prog.delopt(path)
        if not path_opt:
          return "'%s' not specified" % path
        gopts.getstrkey(path, path_opt)

    gopts.getintkey('step', -1)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
