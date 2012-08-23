#!/bin/bash

STREAMING_JAR=/usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u4.jar

hadoop fs -rmr FULL_TESTING_1

hadoop jar $STREAMING_JAR \
  -libjars 'feathers.jar' \
  -input 'B_10.mseq/part-00000' \
  -output FULL_TESTING_1 \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' \
  -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' \
  -jobconf 'stream.reduce.output=typedbytes' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -outputformat 'fm.last.feathers.output.MultipleSequenceFiles' \
  -file tsqr \
  -file tsqr_wrapper.sh \
  -file libblas.so.3 \
  -mapper './tsqr_wrapper.sh map 1' \
  -reducer 'org.apache.hadoop.mapred.lib.IdentityReducer' \
  -numReduceTasks 0

hadoop fs -rmr FULL_TESTING_2

hadoop jar $STREAMING_JAR \
  -libjars 'feathers.jar' \
  -input 'FULL_TESTING_1/R_*' \
  -output FULL_TESTING_2 \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' \
  -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' \
  -jobconf 'stream.reduce.output=typedbytes' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -outputformat 'fm.last.feathers.output.MultipleSequenceFiles' \
  -file tsqr \
  -file tsqr_wrapper.sh \
  -file libblas.so.3 \
  -mapper 'org.apache.hadoop.mapred.lib.IdentityMapper' \
  -reducer './tsqr_wrapper.sh map 2' \
  -numReduceTasks 1

hadoop fs -rmr FULL_TESTING_3

hadoop jar $STREAMING_JAR \
  -libjars 'feathers.jar' \
  -input 'FULL_TESTING_1/Q_*' \
  -output FULL_TESTING_3 \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' \
  -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' \
  -jobconf 'stream.reduce.output=typedbytes' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -file tsqr \
  -file tsqr_wrapper.sh \
  -file libblas.so.3 \
  -mapper './tsqr_wrapper.sh map 3' \
  -reducer 'org.apache.hadoop.mapred.lib.IdentityReducer' \
  -numReduceTasks 0
