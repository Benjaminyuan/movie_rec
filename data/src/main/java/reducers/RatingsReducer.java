package reducers;

import dto.Info;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RatingsReducer extends Reducer<LongWritable, Info, LongWritable, Info> {
    @Override
    protected void reduce(LongWritable key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
        // 电影的tag列表
        List<Info> tagList = new ArrayList<Info>();
        // 电影的打分列表
        List<Info> ratingList = new ArrayList<>();
        values.forEach(info -> {
            if(info.isRatingData()){
                ratingList.add(info);
            }else{
                tagList.add(info);
            }
        });
        ratingList.forEach(info -> {
            tagList.forEach(tagInfo ->{
                // 记录用户的打分和打分电影对应的tag
                tagInfo.setRating(info.getRating());
                tagInfo.setUserId(info.getUserId());
                try {
                    System.out.println("reduce -- " + tagInfo);
                    context.write(new LongWritable(info.getUserId()),tagInfo);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
