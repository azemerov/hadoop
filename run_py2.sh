#!/bin/bash

export MAPRED=/home/hduser/wrk/hadoop/mapred2.py
#export JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
export OUT=/output
export IN="/sales/*"

echo cleanup...
#$HADOOP_HOME/bin/
hdfs dfs -rm -R $OUT

echo run...
#$HADOOP_HOME/bin/hadoop jar $JAR -files $MAPRED -mapper mapred2.py\ -map\ -idx\ 7\ -sep\ ,\ -ne\ Russia -reducer mapred2.py\ -reduce -input $IN -output $OUT
mapred streaming -files $MAPRED -mapper mapred2.py\ -map\ -idx\ 7\ -sep\ ,\ -ne\ Russia -reducer mapred2.py\ -reduce -input $IN -output $OUT
echo cat...
#$HADOOP_HOME/bin/
hdfs dfs -cat $OUT/part-00000

echo run2...
#$HADOOP_HOME/bin/hadoop jar $JAR -files $MAPRED -mapper mapred2.py\ -asis -reducer mapred2.py\ -reduce_max\ -max -input $OUT/part-* -output $OUT/output2
mapred streaming  -files $MAPRED -mapper mapred2.py\ -asis -reducer mapred2.py\ -max -input $OUT/part-* -output $OUT/output2
echo cat2...
#$HADOOP_HOME/bin/
hdfs dfs -cat $OUT/output2/part-00000



