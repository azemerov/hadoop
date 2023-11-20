#!/bin/bash

set MAPPER=/home/hduser_/tutorial2/mapper2.py
set REDUCER=/home/hduser_/tutorial2/reducer.py
set JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
set OUT=/gutenberg-output
set IN="/books/*"

echo rm...
$HADOOP_HOME/bin/hdfs dfs -rm -R $OUT

$HADOOP_HOME/bin/hadoop jar $JAR -files $MAPPER,$REDUCER -mapper $MAPPER -reducer $REDUCER -input $IN -output $OUT

$HADOOP_HOME/bin/hdfs dfs -cat $OUT/part-00000


