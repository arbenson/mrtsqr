#!/bin/bash

STREAMING_JAR='/usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u4.jar'

MATRIX='Simple_1k_10.bseq'
OUTPUT1='INDIRECT_TSQR_TESTING_1'
OUTPUT2='INDIRECT_TSQR_TESTING_2'

hadoop fs -rmr $OUTPUT1

hadoop jar $STREAMING_JAR \
-input $MATRIX \
-output $OUTPUT1 \
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
-mapper './tsqr_wrapper.sh ata' \
-reducer './tsqr_wrapper.sh rowsum'

hadoop fs -rmr $OUTPUT2

hadoop jar $STREAMING_JAR \
-input $OUTPUT1 \
-output $OUTPUT2 \
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
-mapper 'org.apache.hadoop.mapred.lib.IdentityMapper' \
-reducer './tsqr_wrapper.sh cholesky'
