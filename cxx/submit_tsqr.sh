#!/bin/bash

#HADOOP_DIR=/home/mrhadoop/hadoop-0.21.0
#STREAMING_JAR=$HADOOP_DIR/mapred/contrib/streaming/hadoop-0.21.0-streaming.jar
STREAMING_JAR=/usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u4.jar

hadoop fs -rmr test-sumsq

hadoop jar $STREAMING_JAR -input A_800M_10.bseq -output test-sumsq \
  -jobconf mapreduce.job.name=tsqr_cxx \
  -jobconf 'stream.map.input=typedbytes' \
  -jobconf 'stream.reduce.input=typedbytes' \
  -jobconf 'stream.map.output=typedbytes' \
  -jobconf 'stream.reduce.output=typedbytes' \
  -outputformat 'org.apache.hadoop.mapred.SequenceFileOutputFormat' \
  -inputformat 'org.apache.hadoop.streaming.AutoInputFormat' \
  -file tsqr \
  -file tsqr_wrapper.sh \
  -file libblas.so.3 \
  -mapper "./tsqr_wrapper.sh map" \
  -reducer 'org.apache.hadoop.mapred.lib.IdentityReducer' \

hadoop jar $STREAMING_JAR dumptb test-sumsq | ./dump_typedbytes_info -  
