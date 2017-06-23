package ru.mipt.bigdata.Utils;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PermutationWithCountersInputFormat extends
        FileInputFormat<WordCountPair, LongWritable> {

    @Override
    public RecordReader<WordCountPair, LongWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new PermutationWithCountersRecordReader();
    }
}
