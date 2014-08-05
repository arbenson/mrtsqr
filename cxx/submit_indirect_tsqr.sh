#!/bin/bash
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

STREAMING_JAR='/usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u4.jar'

MATRIX='Simple_1k_10.bseq'
OUTPUT='INDIRECT_TSQR_TESTING'

hadoop fs -rmr $OUTPUT

hadoop jar $STREAMING_JAR \
-input $MATRIX \
-output $OUTPUT \
-jobconf 'mapreduce.job.name=tsqr_cxx' \
-jobconf 'stream.map.input=typedbytes' \
-jobconf 'stream.reduce.input=typedbytes' \
-jobconf 'stream.map.output=typedbytes' \
-jobconf 'stream.reduce.output=typedbytes' \
-outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
-inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
-file 'tsqr' \
-file 'tsqr_wrapper.sh' \
-numReduceTasks 1 \
-mapper './tsqr_wrapper.sh indirect' \
-reducer './tsqr_wrapper.sh indirect'
