package ru.mipt.bigdata.Utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UsersCountPartitioner extends Partitioner<PostInfo, LongWritable> {
    @Override
    public int getPartition(PostInfo postInfo, LongWritable longWritable, int numPartitions) {
        int hashCode = (int) (postInfo.getPostId().get() + postInfo.getPostTypeId().get());
        return (hashCode & Integer.MAX_VALUE) % numPartitions;
    }
}
