package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.bigdata.Utils.TaggedUserId;

public abstract class JoinMapper extends Mapper<LongWritable, Text, TaggedUserId, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(new TaggedUserId(value, getTag()), getTag());
    }

    // Не получилось реализовать получение тега через context в setup, как было на лекции
    // Происходил java.lang.ClassCastException:
    // Caused by: org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit cannot be cast
    //  to org.apache.hadoop.mapreduce.lib.input.FileSplit
    // Сделал такой workaround, в котором к тому же экономим на работе с файловой системой
    abstract Text getTag();
}
