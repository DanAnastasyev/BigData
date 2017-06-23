package ru.mipt.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mipt.bigdata.Utils.PermutationWithCountersInputFormat;
import ru.mipt.bigdata.Utils.TextPair;
import ru.mipt.bigdata.Utils.WordAndPermutationComparator;
import ru.mipt.bigdata.Utils.WordCountPair;

public class PermutationsCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PermutationsCountDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path tmpPath = new Path(new Path(args[1]).getParent(), "tmp");
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        try {
            // Собираем статистику по перестановкам
            Job countingJob = new Job(conf, "Permutations count");
            countingJob.setJarByClass(PermutationsCountDriver.class);
            countingJob.setMapperClass(PermutationsCountMapper.class);
            countingJob.setReducerClass(PermutationsCountReducer.class);
            countingJob.setGroupingComparatorClass(WordAndPermutationComparator.class);

            countingJob.setInputFormatClass(TextInputFormat.class);
            countingJob.setOutputFormatClass(TextOutputFormat.class);

            countingJob.setMapOutputKeyClass(TextPair.class);
            countingJob.setMapOutputValueClass(WordCountPair.class);
            countingJob.setNumReduceTasks(4);

            FileInputFormat.addInputPath(countingJob, new Path(args[0]));
            FileOutputFormat.setOutputPath(countingJob, tmpPath);

            if (!countingJob.waitForCompletion(true)) {
                return 1;
            }

            // Сортируем перестановки
            int numOfSortingReducers = 4;
            Job sortingJob = new Job(conf, "Sorting");
            sortingJob.setJarByClass(PermutationsCountDriver.class);
            sortingJob.setMapperClass(Mapper.class);
            sortingJob.setReducerClass(Reducer.class);
            sortingJob.setPartitionerClass(TotalOrderPartitioner.class);
            sortingJob.setNumReduceTasks(numOfSortingReducers);
            sortingJob.setInputFormatClass(PermutationWithCountersInputFormat.class);
            sortingJob.setOutputFormatClass(TextOutputFormat.class);

            sortingJob.setMapOutputKeyClass(WordCountPair.class);
            sortingJob.setMapOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(sortingJob, tmpPath);
            FileOutputFormat.setOutputPath(sortingJob, new Path(args[1]));

            sortingJob.setPartitionerClass(TotalOrderPartitioner.class);

            Path partitionFile = new Path(tmpPath, "partitioning");
            TotalOrderPartitioner.setPartitionFile(sortingJob.getConfiguration(), partitionFile);

            InputSampler.Sampler sampler =
                    new InputSampler.RandomSampler(0.01, 1000, 100);
            InputSampler.writePartitionFile(sortingJob, sampler);

            if (!sortingJob.waitForCompletion(true)) {
                return 1;
            }

            displayResults(args[1], fs);

            return 0;
        } finally {
            try {
                fs.delete(tmpPath, true);
            } catch (Throwable ignored) {
            }
        }
    }

    // Выводит топ-10 записей на экран. Цикл нужен на случай, если записей было мало
    //  и топ-10 нужно искать по нескольким part-r-*
    private void displayResults(String path, FileSystem fs) throws IOException {
        int readedRecords = 0;
        FileStatus[] reduceResults = fs.listStatus(new Path(path), new PathFilter() {
            public boolean accept(Path file) {
                return file.getName().startsWith("part-r");
            }
        });

        for (FileStatus reduceResult : reduceResults) {
            FSDataInputStream in = fs.open(reduceResult.getPath());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line = reader.readLine();
                while (readedRecords < 10 && line != null) {
                    System.out.println(line);
                    line = reader.readLine();
                    ++readedRecords;
                }
                if (readedRecords >= 10) {
                    break;
                }
            }
        }
    }
}
