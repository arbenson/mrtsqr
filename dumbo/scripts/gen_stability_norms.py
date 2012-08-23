#!/usr/bin/env python

import numpy
import util

cm = util.CommandManager()
norms = [[], [], []]
for j in xrange(3):
    for i in xrange(17):
        if j == 0:
            mat = 'Stability_caqr_%d_Q' % i
        elif j == 1:
            mat = 'Stability_caqr_%d_IR_Q' % i
        elif j == 2:
            mat = 'Stability_full_%d_3' % i
        mat += '-ata'
        local_store = mat + '_local'
        cm.copy_from_hdfs(mat, local_store + '.mseq')
        cm.parse_seq_file(local_store + '.mseq', local_store + '.txt')
        data = []
        for line in util.parse_matrix_txt(local_store + '.txt'):
            data.append(line)
        matrix = numpy.mat(data) - numpy.identity(10)
        norm = numpy.linalg.norm(matrix, 2)
        print norm
        norms[j].append(norm)

for norm_list in norms:
    print norm_list

