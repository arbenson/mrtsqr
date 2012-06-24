#!/usr/bin/env python

'''
This script is used to form the eigen decomposition needed for the DMD data.

Example usage:
python form_Y.py --S=MATMUL_OUT --Vt=SVD/Vt --Sigma=SVD/Sigma -o DMD

Use:
  python form_Y.py --help
for more information on how to use the program.

S = U^TA^{(n)}
A^{(n-1)} = U*Sigma*V^t

Let S_tilde = S*V*Sigma^{-1} and S_tilde = Y*Lambda (eigenvalue decomposition).
Then the outputs are two n by n matrices, Re(Y) and Im(Y), i.e. the matrices
consisting of the real pats and imaginary parts of the elements of Y.

Copyright (c) 2012
Austin R. Benson arbenson@gmail.com
'''

import os
import sys
import numpy
import subprocess
from optparse import OptionParser

# print error messages and exit with failure
def error(msg):
  print msg
  sys.exit(1)

# simple wrapper around printing with verbose option
def output(msg):
  print msg

# simple wrapper for parsing a sequence file
def parse_seq_file(inp):
  parse_cmd = 'python hyy-python-hadoop/examples/SequenceFileReader.py %s > %s' % (inp, inp + '.out')
  exec_cmd(parse_cmd)

# simple wrapper for executing command line programs
def exec_cmd(cmd):
  output('(command is: %s)' % (cmd))
  retcode = subprocess.call(cmd,shell=True)
  # TODO(arbenson): make it more obvious when something fails
  return retcode

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
    error('no S matrix provided, use --S')

matrices['Vt'] = {'path': options.Vt, 'mat': None}
if matrices['Vt']['path'] == '':
    error('no V^t matrix provided, use --Vt')

matrices['Sigma'] = {'path': options.Sigma, 'mat': None}
if matrices['Sigma']['path'] == '':
    error('no Sigma matrix provided, use --Sigma')
out = options.out

for matrix in matrices:
    # remove local file if it exists
    local_store = 'form_Y_' + matrix + '.txt'
    if os.path.exists(local_store):
        os.remove(local_store)

    # copy the data locally
    copy_cmd = 'hadoop fs -copyToLocal %s/part-00000 %s' % (matrices[matrix]['path'], local_store)
    exec_cmd(copy_cmd)

    # read the data
    parse_seq_file(local_store)

    f = open(local_store + '.out', 'r')
    data = []
    for line in f:
      if len(line) > 5:
        ind2 = line.rfind(')')
        line = line[ind2+3:]
        line = line.lstrip('[').rstrip().rstrip(']')
        try:
          line2 = line.split(',')
          line2 = [float(v) for v in line2]
        except:
          line2 = line.split()
          line2 = [float(v) for v in line2]
          data.append(line2)
    f.close()
    matrices[matrix]['mat'] = numpy.mat(data)

S = matrices['S']['mat']
V = numpy.transpose(matrices['Vt']['mat'])
Sigma_inv = numpy.linalg.pinv(matrices['Sigma']['mat'])
S_tilde = S*V*Sigma_inv
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
