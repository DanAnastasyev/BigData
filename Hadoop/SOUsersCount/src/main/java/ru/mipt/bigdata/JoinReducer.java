package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.mipt.bigdata.Utils.TaggedUserId;

public class JoinReducer extends Reducer<TaggedUserId, Text, Text, NullWritable> {
    private Text answersTag;
    private Text questionsTag;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        answersTag = new Text(context.getConfiguration().get("AnswersTag"));
        questionsTag = new Text(context.getConfiguration().get("QuestionsTag"));
    }

    @Override
    protected void reduce(TaggedUserId key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        boolean hasGoodAnswer = false;
        boolean hasGoodQuestion = false;
        for (Text tag : values) {
            if (tag.equals(questionsTag)) {
                hasGoodQuestion = true;
            } else if (tag.equals(answersTag)) {
                hasGoodAnswer = true;
            }
        }
        if (hasGoodAnswer && hasGoodQuestion) {
            context.write(key.getUserId(), NullWritable.get());
            context.getCounter(UsersCountDriver.USER_COUNTER.TOTAL_USERS_COUNT).increment(1);
        }
    }
}
