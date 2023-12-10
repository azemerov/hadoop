#!/bin/bash

export MAPPER=/home/hduser/wrk/hadoop/mapper1.py
export REDUCER=/home/hduser/wrk/hadoop/reducer1.py
export JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
export OUT=/gutenberg-output
export IN="/sales/*"

echo rm...
$HADOOP_HOME/bin/hdfs dfs -rm -R $OUT
echo run...
$HADOOP_HOME/bin/hadoop jar $JAR -files $MAPPER,$REDUCER -mapper $MAPPER -reducer $REDUCER -input $IN -output $OUT
echo cat...
$HADOOP_HOME/bin/hdfs dfs -cat $OUT/part-00000


