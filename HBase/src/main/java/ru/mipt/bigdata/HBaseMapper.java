package ru.mipt.bigdata;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseMapper extends Mapper<LongWritable, Text, DomainHit, LongWritable> {
    private final static LongWritable one = new LongWritable(1);

    private static String getDomainName(String url) throws URISyntaxException {
        URI uri = new URI(url);
        String domain = uri.getHost();
        if (domain != null) {
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } else {
            return url;
        }
    }

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        final String[] fields = line.toString().split("\t");
        final String timestamp = fields[1];
        final String url = fields[2];

        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(timestamp) * 1000));
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);

        try {
            context.write(new DomainHit(year, month, day, getDomainName(url)), one);
        } catch (URISyntaxException e) {
        }
    }
}
