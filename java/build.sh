#!/bin/sh

HADOOP_HOME="/usr/lib/hadoop"

HADOOP_JAR="`echo $HADOOP_HOME/hadoop-*-core.jar`"
STREAMING_JAR="`echo $HADOOP_HOME/contrib/streaming/hadoop-*-streaming.jar`"
#LOGGING_JAR="`echo $HADOOP_HOME/lib/commons-logging-*.jar`"
LOGGING_JAR="/usr/lib/hadoop/lib/commons-logging-1.0.4.jar"

CLASSES="$HADOOP_JAR:$STREAMING_JAR:$LOGGING_JAR"
echo $CLASSES

rm -rf classes 2> /dev/null
rm FixedLengthInputFormat.jar 2> /dev/null
mkdir classes
javac -classpath "$CLASSES" -d classes src/org/apache/hadoop/mapred/lib/*.java
jar -cvf FixedLengthInputFormat.jar -C classes/ .
