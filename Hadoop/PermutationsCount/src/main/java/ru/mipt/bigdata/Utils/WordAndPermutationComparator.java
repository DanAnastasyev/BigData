package ru.mipt.bigdata.Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import ru.mipt.bigdata.Utils.TextPair;

/**
 * Компаратор для группировки ключей из PermutationCountMapper.
 * Ключи группируем, глядя на перестановку, а не на слово
 */
public class WordAndPermutationComparator extends WritableComparator {
    public WordAndPermutationComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TextPair tp1 = (TextPair) w1;
        TextPair tp2 = (TextPair) w2;
        return tp1.getFirst().compareTo(tp2.getFirst());
    }
}