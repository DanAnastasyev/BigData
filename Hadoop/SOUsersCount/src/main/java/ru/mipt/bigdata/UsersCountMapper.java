package ru.mipt.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import ru.mipt.bigdata.Utils.PostInfo;

public class UsersCountMapper extends Mapper<LongWritable, Text, PostInfo, LongWritable> {

    @Override
    public void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        try {
            Element parsedElement = DocumentHelper.parseText(line.toString()).getRootElement();

            String postTypeIdString = parsedElement.attributeValue("PostTypeId");
            if (postTypeIdString == null) {
                return;
            }

            int postTypeId = Integer.parseInt(postTypeIdString);
            if (postTypeId == 1) {
                parseQuestion(parsedElement, context);
            } else if (postTypeId == 2) {
                parseAnswer(parsedElement, context);
            }
        } catch (DocumentException e) {
            // pass
        }
    }

    private void parseQuestion(Element parsedElement, Context context)
            throws IOException, InterruptedException {
        String postIdString = parsedElement.attributeValue("Id");
        String favoriteCountString = parsedElement.attributeValue("FavoriteCount");
        if (postIdString == null || favoriteCountString == null) {
            return;
        }

        long postId = Long.parseLong(postIdString);
        long favoriteCount = Long.parseLong(favoriteCountString);
        if (favoriteCount >= 100) {
            PostInfo postInfo = new PostInfo(postId, 1, 0);
            String userId = parsedElement.attributeValue("OwnerUserId");
            if (userId != null) {
                context.write(postInfo, new LongWritable(Long.parseLong(userId)));
            }
        }
    }

    private void parseAnswer(Element parsedElement, Context context)
            throws IOException, InterruptedException {
        String postIdString = parsedElement.attributeValue("ParentId");
        String scoreString = parsedElement.attributeValue("Score");
        if (postIdString == null || scoreString == null) {
            return;
        }

        long postId = Long.parseLong(postIdString);
        long score = Long.parseLong(scoreString);
        PostInfo postInfo = new PostInfo(postId, 2, score);
        String userId = parsedElement.attributeValue("OwnerUserId");
        if (userId != null) {
            context.write(postInfo, new LongWritable(Long.parseLong(userId)));
        }
    }
}
