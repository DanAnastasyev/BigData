package ru.mipt.bigdata.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class PostInfo implements WritableComparable<PostInfo> {
    private LongWritable postId;
    private IntWritable postTypeId;
    private LongWritable score;

    public PostInfo() {
        this(new LongWritable(), new IntWritable(), new LongWritable());
    }

    public PostInfo(long postId, int postTypeId, long score) {
        this(new LongWritable(postId), new IntWritable(postTypeId), new LongWritable(score));
    }

    public PostInfo(LongWritable postId, IntWritable postTypeId, LongWritable score) {
        this.postId = postId;
        this.postTypeId = postTypeId;
        this.score = score;
    }

    public LongWritable getPostId() {
        return postId;
    }

    public IntWritable getPostTypeId() {
        return postTypeId;
    }

    public LongWritable getScore() {
        return score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        postId.write(out);
        postTypeId.write(out);
        score.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        postId.readFields(in);
        postTypeId.readFields(in);
        score.readFields(in);
    }

    @Override
    public int hashCode() {
        return (int) (postTypeId.get() + postTypeId.get() + score.get());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PostInfo) {
            PostInfo post = (PostInfo) o;
            return postId.equals(post.postId)
                    && postTypeId.equals(post.postTypeId)
                    && score.equals(post.score);
        }
        return false;
    }

    @Override
    public String toString() {
        return postId + "\t" + postTypeId + "\t" + score;
    }

    @Override
    public int compareTo(PostInfo post) {
        int cmp = postId.compareTo(post.postId);
        if (cmp != 0) {
            return cmp;
        }
        cmp = postTypeId.compareTo(postTypeId);
        if (cmp != 0) {
            return cmp;
        }
        return -1 * score.compareTo(post.score);
    }
}
