#!/bin/bash
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar

hadoop fs -rmr test-wc-cc

hadoop jar $STREAMING_JAR -input gutenberg-small/132.txt -output test-wc-cc \
  -jobconf mapreduce.job.name=word_count_cxx \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file word_count -mapper "./word_count map" -reducer "./word_count reduce" \


  
hadoop jar $STREAMING_JAR dumptb test-wc-cc | ./dump_typedbytes_info -  
