#!/bin/bash

hadoop --config /etc/hadoop/conf.empty jar hadoop-streaming.jar \
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

hadoop --config /etc/hadoop/conf.empty jar hadoop-streaming.jar \
-D stream.map.output.field.separator='\t' \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
-D mapreduce.partition.keycomparator.options=-k1,1nr \
-mapper cat \
-file reducer.awk -reducer ./reducer.awk \
-input tmpOutput -output $2

rm -r tmpOutput