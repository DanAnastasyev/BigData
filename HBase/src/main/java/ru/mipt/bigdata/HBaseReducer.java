package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HBaseReducer extends
        Reducer<DomainHit, LongWritable, ImmutableBytesWritable, Mutation> {

    @Override
    protected void reduce(DomainHit hit, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException {

        long hitsCount = 0;
        for (LongWritable hitCounter : values) {
            hitsCount += hitCounter.get();
        }

        byte[] key = Bytes.toBytes(String.format("%04d%02d", hit.getYear().get(), hit.getMonth().get()));
        Put put = new Put(key);
        put.addColumn(Bytes.toBytes(String.format("col_day%02d", hit.getDay().get())),
                Bytes.toBytes(hit.getDomain().toString()),
                Bytes.toBytes(hitsCount));
        context.write(new ImmutableBytesWritable(key), put);
    }
}
