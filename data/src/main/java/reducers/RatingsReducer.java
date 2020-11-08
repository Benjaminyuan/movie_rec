package reducers;

import com.alibaba.fastjson.JSON;
import dto.Info;
import org.apache.avro.data.Json;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RatingsReducer extends Reducer<LongWritable, Info, LongWritable, Text> {
    private Info.MovieInfoBuilder builder = new Info.MovieInfoBuilder();

    @Override
    protected void reduce(LongWritable key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
        // 电影的tag列表
        List<Info> tagList = new ArrayList<>();
        // 电影的打分列表
        List<Info> ratingList = new ArrayList<>();

        for (Info info : values) {
            Info tmp = new Info();
            tmp.setRatingData(info.isRatingData());
            tmp.setMovieId(info.getMovieId());
            if (info.isRatingData()) {
                tmp.setUserId(info.getUserId());
                tmp.setRating(info.getRating());
                tmp.setTimestamp(info.getTimestamp());
                ratingList.add(tmp);
            } else {

                tmp.setTagId(info.getTagId());
                tmp.setRelevance(info.getRelevance());
                tagList.add(tmp);
            }
        }

        ratingList.forEach(info -> {

            tagList.forEach(tagInfo -> {
                // 记录用户的打分和打分电影对应的tag
                tagInfo.setRating(info.getRating() * tagInfo.getRelevance());

                tagInfo.setUserId(info.getUserId());
                System.out.println("userId: " + info.getUserId() + " ratings " + tagInfo.getRating() + " tagId: " + tagInfo.getTagId());

                try {
                    context.write(new LongWritable(info.getUserId()), new Text(JSON.toJSONString(tagInfo)));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
