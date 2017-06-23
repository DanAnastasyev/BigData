package ru.mipt.bigdata.Utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<TaggedUserId, Text> {
    @Override
    public int getPartition(TaggedUserId userId, Text tag, int numPartitions) {
        return (userId.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}