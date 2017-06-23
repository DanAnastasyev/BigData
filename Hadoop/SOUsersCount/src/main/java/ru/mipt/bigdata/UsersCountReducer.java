package ru.mipt.bigdata;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import ru.mipt.bigdata.Utils.PostInfo;

public class UsersCountReducer extends Reducer<PostInfo, LongWritable, LongWritable, NullWritable> {
    private MultipleOutputs<LongWritable, NullWritable> multipleOutputs;
    private String answersFolder;
    private String questionsFolder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
        answersFolder = context.getConfiguration().get("AnswersFolder");
        questionsFolder = context.getConfiguration().get("QuestionsFolder");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void reduce(PostInfo key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        // Благодаря сортировке нам нужен только первый элемент из списка
        // В нем будет либо id автора вопроса (он только один),
        //  либо id автора ответа с наибольшим score
        if (values.iterator().hasNext()) {
            LongWritable userId = values.iterator().next();

            multipleOutputs.write(userId, NullWritable.get(),
                    getOutputFileName(key.getPostTypeId().get()));
        }
    }

    private String getOutputFileName(int postTypeId) {
        if (postTypeId == 1) {
            return questionsFolder + File.separator + "part";
        }
        if (postTypeId == 2) {
            return answersFolder + File.separator + "part";
        }
        throw new IllegalStateException();
    }
}
