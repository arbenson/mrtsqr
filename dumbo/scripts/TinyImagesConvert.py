"""
   Copyright (c) 2012-2014, Austin Benson and David Gleich
   All rights reserved.

   This file is part of MRTSQR and is under the BSD 2-Clause License, 
   which can be found in the LICENSE file in the root directory, or at 
   http://opensource.org/licenses/BSD-2-Clause
"""

"""
TinyImagesConvert.py
===========

Script for converting the tiny images binary file to a
sequence of key-value pairs in HDFS.
"""

import sys
import os
import random
import time

import array
import util
import numpy

import dumbo
import dumbo.backends.common

import struct


# create the global options structure
gopts = util.GlobalOptions()

class TinyImages:
    def unpack_key(self,key):
        key = struct.unpack('>q',key)[0]
        if key%(3*1024) != 0:
            print >>sys.stderr,"Warning, unpacking invalid key %i\n" % (key)
        key = key / (3 * 1024)
        return key
        
    def unpack_value(self,value):
        im = array.array('B', value)
        red = im[0:1024]
        green = im[1024:2048]
        blue = im[2048:3072]
        
        return (red,green,blue)
        
    def sum_rgb(self,value):
        (red,green,blue) = self.unpack_value(value)
        return (sum(red),sum(green),sum(blue))
        
    def togray(self,value):
        """ Convert an input value in bytes into a floating point grayscale array. """
        (red,green,blue) = self.unpack_value(value)
        
        gray = []
        for i in xrange(1024):
            graypx = (0.299 * float(red[i]) + 0.587 * float(green[i]) +
                0.114 * float(blue[i])) / 255.
            gray.append(graypx)
            
        return gray
        
class TinyImagesReader(TinyImages):
    def __init__(self):
        self.data = []
        
    def __call__(self,data):
        for key, value in data:
            key = self.unpack_key(key)
            gray = self.togray(value)
            self.data.append(gray)

        for v in self.data:
            key = [numpy.random.randint(0, 4000000000) for i in range(3)]
            yield key, struct.pack('d' * len(v), *v)
    
def runner(job):
    job.additer(mapper=TinyImagesReader(),
                reducer=TinyImagesReader(),
                opts=[('numreducetasks', '0'),
                      ('inputformat','org.apache.hadoop.mapred.lib.FixedLengthInputFormat'),
                      ('jobconf','mapreduce.input.fixedlengthinputformat.record.length=3072')])

def starter(prog):
    
    print "running starter!"
    
    # set the global opts
    gopts.prog = prog
    
    # determine the split size
    splitsize = prog.delopt('split_size')
    if splitsize is not None:
        prog.addopt('jobconf',
            'mapreduce.input.fileinputformat.split.minsize=' + str(splitsize))

    in_file = '/data/tinyimages/tiny_images.bin'
    out_file = 'ti_gray.bseq'
    mypath = os.path.dirname(__file__)

    prog.addopt('memlimit','8g')
    prog.addopt('file', os.path.join(mypath, 'util.py'))
    prog.addopt('jobconf', 'mapred.output.compress=true')
    prog.addopt('input', in_file)
    prog.addopt('output', out_file)
    prog.addopt('overwrite','yes')
    
    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)

