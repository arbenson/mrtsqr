"""
Householder algorithm for QR (part 1).

Austin R. Benson
David F. Gleich
Copyright (c) 2012-2014
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

    last_step = gopts.getintkey('last_step')

    if step == 0:
        mapper = HouseholderQR.HouseholderMap1(step, last_step)
    else:
        info_file = gopts.getstrkey('info_file')
        w_file = gopts.getstrkey('w_file')
        mapper = HouseholderQR.HouseholderMap1(step, last_step, info_file, w_file)
   

    reducer = mrmc.ID_REDUCER
    job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks','0')])

def starter(prog):
    # set the global opts
    gopts.prog = prog

    mat = mrmc.starter_helper(prog, use_dirtsqr=False, use_house=True)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-HHQR-1%s'%(matname,matext))

    step = int(prog.getopt('step'))

    if step != 0:
      for path in ['info_file', 'w_file']:
        path_opt = prog.delopt(path)
        if not path_opt:
          return "'%s' not specified" % path
        prog.addopt('file', os.path.join(os.path.dirname(__file__), path_opt))
        gopts.getstrkey(path, path_opt)

    gopts.getintkey('step', -1)
    gopts.getintkey('last_step', 0)

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
