#!/bin/bash

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
-mapper './tsqr_wrapper.sh cholesky' \
-reducer 'org.apache.hadoop.mapred.lib.IdentityReducer'
