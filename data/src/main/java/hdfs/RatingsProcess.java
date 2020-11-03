package hdfs;

import MapReducers.RatingsMP;
import dto.Info;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class RatingsProcess {
    //把ratings.csv数据处理成userId,List<MovieInfo>的形式
    public void mapUserRating() throws IOException, ClassNotFoundException, InterruptedException {
        try{
            Configuration conf = new Configuration();
            conf.addResource("../../../conf/core-site.xml");
            conf.set("fs.default.name","hdfs://localhost:9000");
            Path outputPath = new Path("/output");
            outputPath.getFileSystem(conf).delete(outputPath, true);
            Job job = Job.getInstance(conf,"rating");
            job.setInputFormatClass(TextInputFormat.class);

            job.setJarByClass(RatingsMP.class);
            job.setMapperClass(RatingsMP.RatingsMapper.class);
            job.setReducerClass(RatingsMP.RatingsReducer.class);

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
