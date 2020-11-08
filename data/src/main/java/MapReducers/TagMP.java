package MapReducers;

import hbase.HBaseOperator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class TagMP {
    public static class TagMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println(line);
            String[] list = line.split(" ");
            if (list.length >= 2) {
                context.write(new IntWritable(Integer.parseInt(list[1])), new IntWritable(Integer.parseInt(list[0])));
            }
        }
    }

    public static class TagReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("tagId: " + key.get());
            for (IntWritable intWritable : values) {
                HBaseOperator.insert(String.valueOf(intWritable.get()), "f1", "tag", String.valueOf(key.get()), "tag_map");
            }
            HBaseOperator.insert(String.valueOf(key.get()), "f1", "tag", String.valueOf(key.get()), "tag_map");
            context.write(key, NullWritable.get());
        }
    }
}
