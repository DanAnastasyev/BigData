package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseDriver(), args);
    }

    public static void createTable(Admin admin, TableName table) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table);
        for (int i = 1; i <= 31; ++i) {
            HColumnDescriptor coldef = new HColumnDescriptor(String.format("col_day%02d", i));
            desc.addFamily(coldef);
        }
        admin.createTable(desc);
    }

    @Override
    public int run(String[] args) throws Exception {
        final String table = args[0];

        Configuration hbaseConf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(table);
        if (!admin.tableExists(tableName)) {
            createTable(admin, tableName);
        }

        HTable hTable = new HTable(hbaseConf, tableName);

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "Table builder");
        job.setJarByClass(HBaseDriver.class);
        job.setMapperClass(HBaseMapper.class);
        job.setReducerClass(HBaseReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(DomainHit.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Mutation.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
