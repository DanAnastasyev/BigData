package ru.mipt.bigdata.Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinGroupingComparator extends WritableComparator {
    public JoinGroupingComparator() {
        super(TaggedUserId.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TaggedUserId userId1 = (TaggedUserId) w1;
        TaggedUserId userId2 = (TaggedUserId) w2;

        return userId1.getUserId().compareTo(userId2.getUserId());
    }
}
