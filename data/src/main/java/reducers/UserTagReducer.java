package reducers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dto.Info;
import hbase.HBaseOperator;
import hbase.Status;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UserTagReducer extends Reducer<LongWritable, Info, LongWritable, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> tagMap = new HashMap<>();
        // 聚合分数
        System.out.println("reduce");

        values.forEach(info -> {
            int tagId = Integer.parseInt(HBaseOperator.get(String.valueOf(info.getTagId()), "tag_map").get("f1:tag"));
            tagMap.put(tagId, (info.getRating() + tagMap.getOrDefault(tagId, 0.0)));
//            tagMap.put(info.getTagId(), (info.getRating() + tagMap.getOrDefault(info.getTagId(), 0.0)));
        });
//        // 得到最大最小分数
//        double xMin = Double.MAX_VALUE;
//        double xMax = Double.MIN_VALUE;
//        for (Integer tag : tagMap.keySet()) {
//            System.out.println("tagId: " + tag + ": " + tagMap.get(tag));
//            xMin = Math.min(tagMap.get(tag), xMin);
//            xMax = Math.max(tagMap.get(tag), xMax);
//        }
//        double len = xMax - xMin;
//
//        // 归一化
//        System.out.println("max: " + xMax);
//        System.out.println("min: " + xMin);
//        System.out.println("len: " + len);
//        for (Integer tag : tagMap.keySet()) {
//            tagMap.put(tag, (tagMap.get(tag) - xMin) / len);
//        }
        Status status = HBaseOperator.insert(String.valueOf(key.get()), "f1", "tags", JSON.toJSONString(tagMap), "user_tags");
        System.out.println(key.get() + " \n" + JSON.toJSONString(tagMap));
        if (!status.isOk()) {
            System.out.println("fail to insert data");
        }
        context.write(key, NullWritable.get());
    }
}
