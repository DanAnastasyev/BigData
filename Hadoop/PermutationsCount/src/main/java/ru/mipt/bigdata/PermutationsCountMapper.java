package ru.mipt.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.mipt.bigdata.Utils.TextPair;
import ru.mipt.bigdata.Utils.WordCountPair;

public class PermutationsCountMapper extends Mapper<LongWritable, Text, TextPair, WordCountPair> {
    private final static LongWritable one = new LongWritable(1);

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] words = line.toString().split("[\\p{Punct}\\s]+");
        for (String word : words) {
            if (word.length() < 3) {
                continue;
            }
            // Преобразуем слово в перестановку (отсортированные символы слова)
            char[] chars = word.toLowerCase().toCharArray();
            Arrays.sort(chars);

            TextPair key = new TextPair(new String(chars), word);
            WordCountPair value = new WordCountPair(new Text(word), one);
            context.write(key, value);
        }
    }
}
