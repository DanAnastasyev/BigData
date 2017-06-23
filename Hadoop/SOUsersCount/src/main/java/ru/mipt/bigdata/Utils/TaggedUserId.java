package ru.mipt.bigdata.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TaggedUserId implements WritableComparable<TaggedUserId> {
    private Text userId;
    private Text tag;

    public TaggedUserId() {
        this(new Text(), new Text());
    }

    public TaggedUserId(Text userId, Text tag) {
        this.userId = userId;
        this.tag = tag;
    }

    public Text getUserId() {
        return userId;
    }

    public Text getTag() {
        return tag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userId.write(out);
        tag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        tag.readFields(in);
    }

    @Override
    public int hashCode() {
        return userId.hashCode() << 5 + tag.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TaggedUserId) {
            TaggedUserId user = (TaggedUserId) o;
            return userId.equals(user.userId)
                    && tag.equals(user.tag);
        }
        return false;
    }

    @Override
    public String toString() {
        return userId + "\t" + tag;
    }

    @Override
    public int compareTo(TaggedUserId user) {
        int cmp = userId.compareTo(user.userId);
        if (cmp != 0) {
            return cmp;
        }
        return tag.compareTo(user.tag);
    }

}
