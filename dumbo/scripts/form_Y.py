"""
This script is used to form the eigen decomposition needed for the DMD data.

See options:
     python form_Y.py --help

Example usage:
     python form_Y.py --S=MATMUL_OUT --Vt=SVD/Vt --Sigma=SVD/Sigma -o DMD

S = U^TA^{(n)}
A^{(n-1)} = U*Sigma*V^t

Let S_tilde = S*V*Sigma^{-1} and S_tilde = Y*Lambda (eigenvalue decomposition).
Then the outputs are two n by n matrices, Re(Y) and Im(Y), i.e. the matrices
consisting of the real pats and imaginary parts of the elements of Y.

Copyright (c) 2012-2014
Austin R. Benson
David F. Gleich
"""

from optparse import OptionParser
import numpy
import os
import subprocess
import sys
import util

cm = util.CommandManager()

parser = OptionParser()
parser.add_option('-s', '--S', dest='S', default='',
                  help='path to S = U^T*A^{(n)}')
parser.add_option('-v', '--Vt', dest='Vt', default='',
                  help='path to V^t from A^{(n-1)} = U*Sigma*V^t')
parser.add_option('-e', '--Sigma', dest='Sigma', default='',
                  help='path to Sigma from A^{(n-1)} = U*Sigma*V^t')
parser.add_option('-o', '--output', dest='out', default='DMD_Y',
                  help='base name for output files')
(options, args) = parser.parse_args()

matrices = {}
matrices['S'] = {'path': options.S, 'mat': None}
if matrices['S']['path'] == '':
    cm.error('no S matrix provided, use --S')

matrices['Vt'] = {'path': options.Vt, 'mat': None}
if matrices['Vt']['path'] == '':
    cm.error('no V^t matrix provided, use --Vt')

matrices['Sigma'] = {'path': options.Sigma, 'mat': None}
if matrices['Sigma']['path'] == '':
    cm.error('no Sigma matrix provided, use --Sigma')
out = options.out

for matrix in matrices:
    # copy the data over and parse it
    local_store = 'form_Y_' + matrix
    cm.copy_from_hdfs(matrices[matrix]['path'], local_store + '.mseq')
    cm.parse_seq_file(local_store + '.mseq', local_store + '.txt')

    # read the local data
    data = []
    for line in util.parse_matrix_txt(local_store + '.txt'):
      data.append(line)

    matrices[matrix]['mat'] = numpy.mat(data)

S = matrices['S']['mat']
V = numpy.transpose(matrices['Vt']['mat'])
Sigma_inv = numpy.linalg.pinv(matrices['Sigma']['mat'])
S_tilde = S * V * Sigma_inv
eig_vals, Y = numpy.linalg.eig(S_tilde)
Y_Re = numpy.real(Y)
Y_Im = numpy.imag(Y)

def write_Y(mat, path):
  if os.path.exists(path):
    os.remove(path)
  f = open(path, 'w')
  for i, row in enumerate(mat.getA()):
    # deal with annoying new lines
    val_list = str(row).split('\n')
    val_str = ''
    for val in val_list:
      val_str += val
    f.write('(%d) %s\n' %(i, val_str))
  f.close()

write_Y(Y_Re, out + '_Re.txt')
write_Y(Y_Im, out + '_Im.txt')
