#!/usr/bin/env dumbo

"""
Householder mrtsqr

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import struct
import numpy
import math

import util
import mrmc
import sys

import dumbo
import dumbo.backends.common
from dumbo import opt

def parse_info(f):
  PICKED_SET = '(picked_set) '
  ALPHA = '(alpha) '
  TAU = '(tau) '
  SIGMA = '(sigma) '
  
  info = {PICKED_SET: None, ALPHA: None, TAU: None, SIGMA: None}
  
  # TODO(arbenson): This is ugly.  The file is small and specific, so things
  # are annoying to parse.  Make this cleaner.
  for key in info:
    info_data = open(f, 'r')
    for line in info_data:
      if len(line) > len(key) and line[:len(key)] == key:
        # Handle doubles
        if key != PICKED_SET:
          info[key] = float(line[len(key):])
        else:
          line = line[len(key):]
          line = line.lstrip('[').rstrip(']\n').split(',')
          line = [k.lstrip().lstrip('\'').rstrip('\'') for k in line]
          info[key] = line
    info_data.close()

  return (info[PICKED_SET], info[ALPHA], info[TAU], info[SIGMA])


@opt("getpath", "yes")
class HouseholderMap1(mrmc.MatrixHandler):
  def __init__(self, step, last_step, info_file=None, w_file=None, blocksize=500):
    mrmc.MatrixHandler.__init__(self)
    self.data = 0.0
    self.A_data = []

    # step of alg: i = 0, 1, ..., n-1
    self.step = step
    self.first_step = (step == 0)
    self.last_step = last_step

    if self.first_step:
      self.picked_set = []
    else:
      self.picked_set, self.alpha, self.tau, self.sigma = parse_info(info_file)
      self.last_picked = self.picked_set[-1]
      self.w = self.parse_w(w_file)

    self.nrows = 0
    self.blocksize = blocksize
    self.first_key = None
    self.first_value = None

  def parse_w(self, f):
    w = []
    for line in open(f, 'r'):
      ind = line.find(')') + 2
      w.append(float(line[ind:]))
    return w

  def store(self, key, value):
    if self.last_step:
      if key in self.picked_set:
        self.A_data.append((key, value))
    else:
      if key not in self.picked_set:
        if self.first_key is None:
          self.first_key = key
          self.first_value = value[self.step]
        else:
          self.data += value[self.step] * value[self.step]
      self.A_data.append((key, value))

  def collect(self, key, value):
    new_row = [float(v) for v in value]
    if not self.first_step:
      if key == self.last_picked:
        new_row[self.step - 1] = self.alpha
      elif key not in self.picked_set:
        new_row[self.step - 1] *= self.sigma
    
    if not self.first_step:
      last_picked = self.picked_set[-1]
      w = self.w[self.step:]
      if key in self.picked_set:
        if key != last_picked:
          for i, val in enumerate(w):
            new_row[self.step + i] -= val * self.tau
      else:
        for i, val in enumerate(w):
          new_row[self.step + i] -= val * self.tau * new_row[self.step - 1]
  
    self.nrows += 1
    # write status updates so Hadoop doesn't complain
    if self.nrows % 50000 == 0:
      self.counters['rows processed'] += 50000
    self.store(key, new_row)

  def flush(self, last_flush=False):
    for pair in self.A_data:
      key, value = pair
      if not self.last_step:
        value = struct.pack('d' * len(value), *value)
      yield ('A_matrix', key), value
    self.A_data = []

    if not self.last_step and last_flush:
      if self.first_value is not None:
        yield ('KV_output', self.first_key), [self.first_value, '']
      if self.data != 0.0:
        key = numpy.random.randint(0, 4000000000)
        yield ('KV_output', key), [self.data]

    self.nrows = 0

  def __call__(self, data):
    for key, value in data:
      self.collect_data_instance(key, value)
      if self.nrows == self.blocksize:
        for key, val in self.flush():
          yield key, val
    for key, val in self.flush(last_flush=True):
      yield key, val

class HouseholderRed2(dumbo.backends.common.MapRedBase):
  def __init__(self, step, info_file):
    if step == 0:
      self.picked_set = []
    else:
      self.picked_set, _, _, _ = parse_info(info_file)
    self.picked = None
    self.alpha = None
    self.norm = 0.0

  def collect(self, key, value, picked_possibility=False):
    if picked_possibility:
      if self.picked is None:
        self.picked = key
        self.alpha = value
      self.norm += value * value
    else:
      self.norm += value
    
  def close(self):
    beta = math.sqrt(self.norm)
    beta = float(-1.0) * math.copysign(beta, self.alpha)
    tau = (beta - self.alpha) / beta
    sigma = float(1.0) / (self.alpha - beta)
    self.alpha = beta
    self.picked_set.append(self.picked)
    yield 'picked_set', self.picked_set
    yield 'alpha', self.alpha
    yield 'tau', tau
    yield 'sigma', sigma

  def __call__(self, data):
    for key, values in data:
      for value in values:
        self.collect(key, float(value[0]), len(value) == 2)

    for key, val in self.close():
      yield key, val

class HouseholderMap3(mrmc.MatrixHandler):
  def __init__(self, step, info_file):
    mrmc.MatrixHandler.__init__(self)
    self.output_vals = {}
    self.step = step
    self.picked_set, self.alpha, _, self.sigma = parse_info(info_file)
    self.last_picked = self.picked_set[-1]
    self.nrows = 0
    self.first_row_done = False

  def collect(self, key, value):
    value = list(value)
    if key == self.last_picked:
      value[self.step] = self.alpha
    elif key not in self.picked_set:
      value[self.step] *= self.sigma
    self.nrows += 1

    # write status updates so Hadoop doesn't complain
    if self.nrows % 50000 == 0:
      self.counters['rows processed'] += 50000

    if not self.first_row_done:
      for i in xrange(self.step + 1, self.ncols):
        self.output_vals[i] = 0.0
      self.first_row_done = True

    
    if key in self.picked_set:
      if key == self.last_picked:
        for i, val in enumerate(value[self.step + 1:]):
          k = self.step + 1 + i
          self.output_vals[k] += val
    else:
      mult = value[self.step]
      for i, val in enumerate(value[self.step + 1:]):
        k = self.step + 1 + i
        self.output_vals[k] += mult * val

  def close(self):
    for key in self.output_vals:
      yield key, self.output_vals[key]

  def __call__(self, data):
    self.collect_data(data)
    for key, val in self.close():
      yield key, val
    
class HouseholderRed3(dumbo.backends.common.MapRedBase):
  def __init__(self):
    pass

  def __call__(self, data):
    for key, values in data:
      yield key, sum(values)
