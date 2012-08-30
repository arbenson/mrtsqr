#!/usr/bin/env dumbo

"""
Householder mrtsqr

Austin R. Benson (arbenson@stanford.edu)
David F. Gleich
Copyright (c) 2012
"""

import sys
import time
import struct
import uuid

import numpy
import math

import util
import mrmc

import dumbo
import dumbo.backends.common
from dumbo import opt

@opt("getpath", "yes")
class HouseholderMap1(mrmc.MatrixHandler):
  def __init__(self, step, tau, picked_set, w, last_step=False, blocksize=500):
    mrmc.MatrixHandler.__init__(self)
    self.data = []
    self.A_data = []

    # step of alg: i = 0, 1, ..., n-1
    self.step = step
    self.first_step = (step == 0)
    self.last_step = last_step

    self.tau = tau
    self.picked_set = picked_set
    self.w = w

    self.nrows = 0
    self.blocksize = blocksize    

  def store(self, key, value):
    if key not in self.picked_set and not self.last_step:
      self.data.append((key, value[self.step]))
    self.A_data.append((key, value))

  def collect(self, key, value):
    new_row = [float(v) for v in value]
    
    if not self.first_step:
      last_picked = self.picked_set[-1]
      w = self.w[self.step:]
      if key in self.picked_set:
        if key != last_picked:
          for i, val in enumerate(self.tau * w):
            new_row[self.step + i] -= val
      else:
        for i, val in enumerate([v * self.tau * new_row[self.step - 1] for v in w]):
          new_row[self.step + i] -= val
  
    self.nrows += 1
    self.store(key, new_row)

  def flush(self):
    for pair in self.A_data:
      key, value = pair
      yield ('A_matrix', key), value
    self.A_data = []

    if not self.last_step:
      for pair in self.data:
        key, value = pair
        yield ('KV_output', key), value
      self.data = []

    self.nrows = 0

  def __call__(self, data):
    for key, value in data:
      self.collect_data_instance(key, value)
      if self.nrows == self.blocksize:
        for key, val in self.flush():
          yield key, val
    for key, val in self.flush():
      yield key, val

class HouseholderRed2(dumbo.backends.common.MapRedBase):
  def __init__(self, picked_set):
    self.picked_set = picked_set
    self.picked = None
    self.alpha = None
    self.data = []
    
  def collect(self, key, value):
    if self.picked is None:
      self.picked = key
    if self.alpha is None:
      self.alpha = value
    # for now, no blocking scheme here
    self.data.append(value)
    
  def close(self):
    eta = numpy.linalg.norm(self.data, 2)
    beta = math.sqrt(eta * eta)
    beta = -1 * math.copysign(beta, self.alpha)
    tau = (beta - self.alpha) / beta
    sigma = float(1) / (self.alpha - beta)
    alpha = beta
    self.picked_set.append(self.picked)
    yield 'picked_set', self.picked_set
    yield 'alpha', self.alpha
    yield 'tau', tau
    yield 'sigma', sigma

  def __call__(self, data):
    for key, values in data:
      for value in values:
        self.collect(key, float(value))

    for key, val in self.close():
      yield key, val

@opt("getpath", "yes")
class HouseholderMap3(mrmc.MatrixHandler):
  def __init__(self, alpha, sigma, step, picked_set):
    mrmc.MatrixHandler.__init__(self)
    self.data = []
    self.keys = []
    self.output_keys = []
    self.output_vals = []
    self.alpha = alpha
    self.sigma = sigma
    self.step = step
    self.picked_set = picked_set
    self.last_picked = picked_set[-1]

  def collect(self, key, value):
    self.keys.append(key)
    if key == self.last_picked:
      value[self.step] = self.alpha
    elif key not in self.picked_set:
      value[self.step] *= self.sigma
    self.data.append(value)

  def close(self):
    for i, row in enumerate(self.data):
      key = self.keys[i]
      if key in self.picked_set:
        if key != self.last_picked:
          continue
        else:
          self.output_keys += range(self.step + 1, self.ncols)
          self.output_vals += row[self.step + 1:]
      else:
        self.output_keys += range(self.step + 1, self.ncols)
        self.output_vals += [v * row[self.step] for v in row[self.step + 1:]]

  def __call__(self, data):
    self.collect_data(data)
    self.close()

    # output the matrix A
    assert(len(self.keys) == len(self.data))
    for i, key in enumerate(self.keys):
      yield ('A_matrix', key), self.data[i]

    # output the key, value pairs for the reduce
    assert(len(self.output_keys) == len(self.output_vals))
    for i, key in enumerate(self.output_keys):
      yield ('KV_output', key), self.output_vals[i]
    
class HouseholderRed4(dumbo.backends.common.MapRedBase):
  def __init__(self):
    pass
  def __call__(self, data):
    for key, values in data:
      yield key, sum(values)

