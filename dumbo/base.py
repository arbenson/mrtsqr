#!/usr/bin/env python

"""
base.py
===========

Austin R. Benson
copyright (c) 2012
"""

import dumbo
import dumbo.backends.common
import struct

class DataFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MatrixHandler(dumbo.backends.common.MapRedBase):
    def __init__(self):
        self.ncols = None
        self.unpacker = None
    
    def deduce_string_type(self, val):
        # first check for TypedBytes list/vector
        try:
            [float(p) for p in val.split()]
        except:
            if len(val) == 0: return False
            if len(val)%8 == 0:
                ncols = len(val)/8
                # check for TypedBytes string
                try:
                    val = list(struct.unpack('d'*ncols, val))
                    self.ncols = ncols
                    self.unpacker = struct.Struct('d'*ncols)
                    return True
                except struct.error, serror:
                    # no idea what type this is!
                    raise DataFormatException("Data format is not supported.")
            else:
                raise DataFormatException("Number of data bytes ({0}) is not a multiple of 8.".format(len(val)))
                    

