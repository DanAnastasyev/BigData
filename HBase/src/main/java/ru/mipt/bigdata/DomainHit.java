package ru.mipt.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DomainHit implements WritableComparable<DomainHit> {
    private IntWritable year;
    private IntWritable month;
    private IntWritable day;
    private Text domain;

    public DomainHit() {
        set(new IntWritable(), new IntWritable(), new IntWritable(), new Text());
    }

    public DomainHit(int year, int month, int day, String domain) {
        set(new IntWritable(year), new IntWritable(month), new IntWritable(day), new Text(domain));
    }

    public DomainHit(IntWritable year, IntWritable month, IntWritable day, Text domain) {
        set(year, month, day, domain);
    }

    public void set(IntWritable year, IntWritable month, IntWritable day, Text domain) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.domain = domain;
    }

    public IntWritable getYear() {
        return year;
    }

    public IntWritable getMonth() {
        return month;
    }

    public IntWritable getDay() {
        return day;
    }

    public String getDate() {
        return String.format("%04d%02d%02d", year.get(), month.get(), day.get());
    }

    public Text getDomain() {
        return domain;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        month.write(out);
        day.write(out);
        domain.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        month.readFields(in);
        day.readFields(in);
        domain.readFields(in);
    }

    @Override
    public int hashCode() {
        return year.hashCode() + month.hashCode() + day.hashCode() + domain.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DomainHit) {
            DomainHit dh = (DomainHit) o;
            return year.equals(dh.year) && month.equals(dh.month)
                    && day.equals(dh.day) && domain.equals(dh.domain);
        }
        return false;
    }

    @Override
    public String toString() {
        return year.toString() + "\t" + month.toString() + "\t" + day.toString() + "\t" + domain.toString();
    }

    @Override
    public int compareTo(DomainHit dh) {
        int cmp = year.compareTo(dh.year);
        if (cmp != 0) {
            return cmp;
        }
        cmp = month.compareTo(dh.month);
        if (cmp != 0) {
            return cmp;
        }
        cmp = day.compareTo(dh.day);
        if (cmp != 0) {
            return cmp;
        }
        return domain.compareTo(dh.domain);
    }
}
