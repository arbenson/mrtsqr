#!/bin/bash
#   Copyright (c) 2012-2014, Austin Benson and David Gleich
#   All rights reserved.
#
#   This file is part of MRTSQR and is under the BSD 2-Clause License, 
#   which can be found in the LICENSE file in the root directory, or at 
#   http://opensource.org/licenses/BSD-2-Clause

STREAMING_JAR=/usr/common/tig/hadoop/hadoop-0.20.2+228/contrib/streaming/hadoop-0.20.2+228-streaming.jar

hadoop fs -rmr cpp-test-qrr.mseq

hadoop jar $STREAMING_JAR -input $1 -output cpp-test-qrr.mseq -numReduceTasks $2 \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -jobconf 'mapred.child.java.opts=-Xmx2G' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -io typedbytes \
  -file mkl_and_tsqr.sh \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_core.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_intel_lp64.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_sequential.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libiomp5.so \
  -file tsqr \
  -mapper './mkl_and_tsqr.sh map 5 4 house 0' -reducer './mkl_and_tsqr.sh reduce 5 4 house 0'

hadoop fs -rmr cpp-test-house.mseq

hadoop jar $STREAMING_JAR -input cpp-test-qrr.mseq -output cpp-test-house.mseq -numReduceTasks 1 \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' -jobconf 'stream.reduce.output=typedbytes' \
  -jobconf 'mapred.child.java.opts=-Xmx2G' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -io typedbytes \
  -file mkl_and_tsqr.sh \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_core.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_intel_lp64.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libmkl_sequential.so \
  -file /usr/common/usg/mkl/10.2.2.025/lib/em64t/libiomp5.so \
  -file tsqr \
  -mapper './mkl_and_tsqr.sh map 5 4 house 1' -reducer './mkl_and_tsqr.sh reduce 5 4 house 1'
