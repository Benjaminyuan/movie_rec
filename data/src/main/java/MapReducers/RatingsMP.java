package MapReducers;

import dto.Info;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RatingsMP {
    public static class RatingsMapper extends Mapper<LongWritable, Text, LongWritable, Info> {
        private Info.MovieInfoBuilder builder = new Info.MovieInfoBuilder();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: userId,movieId,ratings,timestamp
            String[] line = value.toString().split(",");
            System.out.println(Arrays.toString(line));
            Info info = null;
            LongWritable movieId = null;
            // 长度判断防止越界
            if (line.length == 4) {
                // userId,movieId,rating,timestamp
                System.out.println("评分 " + line[0] + ":" + line[1] + ":" + line[2]);
                movieId = new LongWritable(Long.parseLong(line[1]));
                info = builder.isRatingData(true)
                        .setMovieId(line[1])
                        .setUserId(line[0])
                        .setRating(line[2])
                        .setTimestamp(line[3])
                        .build();
            } else if (line.length == 3) {
                // movieId,tagId,relevance
                System.out.println("相关度" + line[0] + ":" + line[1] + ":" + line[2]);
                movieId = new LongWritable(Long.parseLong(line[0]));
                System.out.printf("movie id: %d\n", movieId.get());
                info = builder.isRatingData(false).setMovieId(line[0])
                        .setTagId(line[1])
                        .setRelevance(line[2])
                        .build();
                System.out.println("相关度 build  ok" + info.toString());
            }
            if (info != null) {
                System.out.println("map --- info : " + info.toString());
                System.out.println("map write start");
                context.write(movieId, info);
                System.out.println("map done");
            }
            System.out.println("info is null");
        }
    }

    public static class RatingsReducer extends Reducer<LongWritable, Info, LongWritable, Info> {
        @Override
        protected void reduce(LongWritable key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
            // 电影的tag列表
            List<Info> tagList = new ArrayList<Info>();
            // 电影的打分列表
            List<Info> ratingList = new ArrayList<>();
            values.forEach(info -> {
                if (info.isRatingData()) {
                    ratingList.add(info);
                } else {
                    tagList.add(info);
                }
            });
            ratingList.forEach(info -> {
                tagList.forEach(tagInfo -> {
                    // 记录用户的打分和打分电影对应的tag
                    tagInfo.setRating(info.getRating());
                    tagInfo.setUserId(info.getUserId());
                    try {
                        System.out.println("reduce -- " + tagInfo);
                        context.write(new LongWritable(info.getUserId()), tagInfo);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });
        }
    }

}