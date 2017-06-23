package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.bigdata.Utils.TextPair;
import ru.mipt.bigdata.Utils.WordCountPair;

public class PermutationsCountReducer
        extends Reducer<TextPair, WordCountPair, WordCountPair, LongWritable> {

    @Override
    protected void reduce(TextPair wordAndPermutationPair, Iterable<WordCountPair> values,
            Context context) throws IOException, InterruptedException {

        // Подсчитываем число уникальных слов и всех слов
        Text prevWord = null;
        long uniqueWordsCount = 0;
        long permutationCount = 0;
        for (WordCountPair wordAndCount : values) {
            if (!wordAndCount.getWord().equals(prevWord)) {
                ++uniqueWordsCount;
                prevWord = new Text(wordAndCount.getWord());
            }
            ++permutationCount;
        }

        WordCountPair key = new WordCountPair(wordAndPermutationPair.getFirst(),
                new LongWritable(permutationCount));
        context.write(key, new LongWritable(uniqueWordsCount));
    }
}