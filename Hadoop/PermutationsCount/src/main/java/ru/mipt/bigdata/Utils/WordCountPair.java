package ru.mipt.bigdata.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Пара: слово и счетчик для него
 */
public class WordCountPair implements WritableComparable<WordCountPair> {
    private Text word;
    private LongWritable count;

    public WordCountPair() {
        this(new Text(), new LongWritable());
    }

    public WordCountPair(String word, long count) {
        this(new Text(word), new LongWritable(count));
    }

    public WordCountPair(Text word, LongWritable count) {
        set(word, count);
    }

    public void set(Text word, LongWritable count) {
        this.word = word;
        this.count = count;
    }

    public Text getWord() {
        return word;
    }

    public LongWritable getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        count.readFields(in);
    }

    @Override
    public int hashCode() {
        return word.hashCode() * 163 + count.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WordCountPair) {
            WordCountPair tp = (WordCountPair) o;
            return word.equals(tp.word) && count.equals(tp.count);
        }
        return false;
    }

    @Override
    public String toString() {
        return word + "\t" + count;
    }

    @Override
    public int compareTo(WordCountPair tp) {
        int cmp = count.compareTo(tp.count);
        if (cmp != 0) {
            return -cmp;
        }
        return word.compareTo(tp.word);
    }
}
