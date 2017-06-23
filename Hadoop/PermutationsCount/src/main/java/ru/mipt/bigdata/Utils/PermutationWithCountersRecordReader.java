package ru.mipt.bigdata.Utils;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class PermutationWithCountersRecordReader extends RecordReader<WordCountPair, LongWritable> {
    private WordCountPair key;
    private LongWritable value;
    private LineRecordReader reader = new LineRecordReader();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            Text line = reader.getCurrentValue();
            String[] fields = line.toString().split("\t");

            try {
                key = new WordCountPair(fields[0], Long.parseLong(fields[1]));
                value = new LongWritable(Long.parseLong(fields[2]));
                return true;
            } catch (Throwable ignored) {
                // Если не получилось распарсить строку - возвращаем false
            }
        }
        key = null;
        value = null;
        return false;
    }

    @Override
    public WordCountPair getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
