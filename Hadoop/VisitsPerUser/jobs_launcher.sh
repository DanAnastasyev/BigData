#!/bin/bash

hadoop jar hadoop-streaming.jar \
-D mapreduce.job.reduces=4 \
-D stream.map.output.field.separator='\t' \
-D stream.num.map.output.key.fields=3 \
-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
-D mapreduce.partition.keycomparator.options=-k1,2 \
-D mapreduce.partition.keypartitioner.options=-k1,1 \
-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
-file mapper.py -mapper ./mapper.py \
-file reducer.py -reducer ./reducer.py \
-input $1 -output tmpOutput

hadoop jar hadoop-streaming.jar \
-D mapreduce.job.reduces=1 \
-D stream.map.output.field.separator='\t' \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
-D mapreduce.partition.keycomparator.options=-k1,1nr \
-mapper cat \
-file reducer.awk -reducer ./reducer.awk \
-input tmpOutput -output $2

hadoop fs -cat $2/part-00000 | head -10

hadoop fs -rm -r tmpOutput
