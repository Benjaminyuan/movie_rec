package hdfs;

import dto.Info;
import mappers.RatingsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducers.RatingsReducer;

import java.io.IOException;

public class RatingsProcess {
    //把ratings.csv数据处理成userId,List<MovieInfo>的形式
    public void mapUserRating() throws IOException, ClassNotFoundException, InterruptedException {
        try{
            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/output"));
            Job job = Job.getInstance(conf,"rating");
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(RatingsMapper.class);
            job.setReducerClass(RatingsReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Info.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Info.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //指定未存在的
            FileInputFormat.addInputPath(job,new Path("hdfs://localhost:9000/input/data/genome-scores.csv"));
            FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/output"));
           System.exit( job.waitForCompletion(true) ? 0 : 1);

            System.out.println("finished -----");
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
