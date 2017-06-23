package ru.mipt.bigdata;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mipt.bigdata.Utils.JoinGroupingComparator;
import ru.mipt.bigdata.Utils.JoinPartitioner;
import ru.mipt.bigdata.Utils.PostInfo;
import ru.mipt.bigdata.Utils.TaggedUserId;
import ru.mipt.bigdata.Utils.UsersCountGroupingComparator;
import ru.mipt.bigdata.Utils.UsersCountPartitioner;

public class UsersCountDriver extends Configured implements Tool {
    private static final Text ANSWERS_TAG = new Text("A");
    private static final Text QUESTIONS_TAG = new Text("Q");
    private static final String ANSWERS_FOLDER = "Answers";
    private static final String QUESTIONS_FOLDER = "Questions";

    private static class AnswersMapper extends JoinMapper {
        @Override
        Text getTag() {
            return ANSWERS_TAG;
        }
    }

    private static class QuestionsMapper extends JoinMapper {
        @Override
        Text getTag() {
            return QUESTIONS_TAG;
        }
    }

    public enum USER_COUNTER {
        TOTAL_USERS_COUNT
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UsersCountDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path tmpPath = new Path(new Path(args[1]).getParent(), "tmp");
        Configuration conf = getConf();

        conf.set("AnswersTag", ANSWERS_TAG.toString());
        conf.set("QuestionsTag", QUESTIONS_TAG.toString());
        conf.set("AnswersFolder", ANSWERS_FOLDER);
        conf.set("QuestionsFolder", QUESTIONS_FOLDER);

        FileSystem fs = FileSystem.get(conf);
        try {
            if (!initParsingJob(conf, new Path(args[0]), tmpPath).waitForCompletion(true)) {
                return 1;
            }

            Job joiningJob = initJoiningJob(conf, tmpPath, new Path(args[1]));
            if (!joiningJob.waitForCompletion(true)) {
                return 2;
            }

            Counter totalUserCounter = joiningJob.getCounters().findCounter(USER_COUNTER.TOTAL_USERS_COUNT);
            System.out.println("Number of users with good questions and answers - " +
                    totalUserCounter.getValue());

            return 0;
        } finally {
            try {
                fs.delete(tmpPath, true);
            } catch (Throwable ignored) {
            }
        }
    }

    // Собираем информацию о пользователях: id тех, у кого есть вопросы с FavoriteCount >= 100,
    //  и id тех, у кого самый высокий score среди ответов на вопрос
    // Записываем их в отдельные папочки (Questions и Answers соответственно)
    private Job initParsingJob(Configuration conf, Path inputPath, Path outputPath)
            throws IOException {

        Job parsingJob = new Job(conf, "Post parsing job");
        parsingJob.setJarByClass(UsersCountDriver.class);
        parsingJob.setMapperClass(UsersCountMapper.class);
        parsingJob.setReducerClass(UsersCountReducer.class);
        parsingJob.setPartitionerClass(UsersCountPartitioner.class);
        parsingJob.setGroupingComparatorClass(UsersCountGroupingComparator.class);

        parsingJob.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(parsingJob, TextOutputFormat.class);

        parsingJob.setMapOutputKeyClass(PostInfo.class);
        parsingJob.setMapOutputValueClass(LongWritable.class);
        parsingJob.setNumReduceTasks(4);

        FileInputFormat.addInputPath(parsingJob, inputPath);
        FileOutputFormat.setOutputPath(parsingJob, outputPath);

        return parsingJob;
    }

    // Inner join'им пользователей из полученных на предыдущем шаге папок
    private Job initJoiningJob(Configuration conf, Path inputPath, Path outputPath)
            throws IOException {

        Job joiningJob = new Job(conf, "Joining job");
        joiningJob.setJarByClass(UsersCountDriver.class);
        joiningJob.setReducerClass(JoinReducer.class);
        joiningJob.setPartitionerClass(JoinPartitioner.class);
        joiningJob.setGroupingComparatorClass(JoinGroupingComparator.class);

        joiningJob.setMapOutputKeyClass(TaggedUserId.class);
        joiningJob.setMapOutputValueClass(Text.class);
        joiningJob.setNumReduceTasks(4);

        LazyOutputFormat.setOutputFormatClass(joiningJob, TextOutputFormat.class);

        Path answersPath = Path.mergePaths(inputPath, new Path(File.separator + ANSWERS_FOLDER));
        MultipleInputs.addInputPath(joiningJob, answersPath, TextInputFormat.class, AnswersMapper.class);

        Path questionsPath = Path.mergePaths(inputPath, new Path(File.separator + QUESTIONS_FOLDER));
        MultipleInputs.addInputPath(joiningJob, questionsPath, TextInputFormat.class, QuestionsMapper.class);

        FileOutputFormat.setOutputPath(joiningJob, outputPath);

        return joiningJob;
    }
}
