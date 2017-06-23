package ru.mipt.bigdata.Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UsersCountGroupingComparator extends WritableComparator {
    public UsersCountGroupingComparator() {
        super(PostInfo.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PostInfo postInfo1 = (PostInfo) w1;
        PostInfo postInfo2 = (PostInfo) w2;
        int cmp = postInfo1.getPostId().compareTo(postInfo2.getPostId());
        if (cmp != 0) {
            return cmp;
        }
        return postInfo1.getPostTypeId().compareTo(postInfo2.getPostTypeId());
    }
}
