package mappers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import dto.Info;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class UserTagMapper extends Mapper<LongWritable, Text, LongWritable, Info> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//         String line = new String(value.getBytes());
        String line = value.toString();
        int index = line.indexOf('\t');
        long userId = Long.parseLong(line.substring(0, index));
        String infoStr = line.substring(index + 1);
        Info info = JSON.parseObject(infoStr, new TypeReference<Info>() {
        });
        context.write(new LongWritable(userId), info);
    }
}
