package hdfs;

import MapReducers.RatingsMP;
import dto.Info;
import mappers.RatingsMapper;
import mappers.UserTagMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.RatingsReducer;
import reducers.UserTagReducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import task.Main;

import java.io.IOException;

public class RatingsProcess {
    //把ratings.csv数据处理成userId,List<MovieInfo>的形式
    public void mapUserRating() throws IOException, ClassNotFoundException, InterruptedException {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "rating");
            job.setJarByClass(Main.class);
            conf.set("fs.default.name", "hdfs://localhost:9000");
            Path outputPath = new Path("/output");
            outputPath.getFileSystem(conf).delete(outputPath, true);
            job.setInputFormatClass(TextInputFormat.class);

            job.setJarByClass(RatingsMP.class);
            job.setMapperClass(RatingsMP.RatingsMapper.class);
            job.setReducerClass(RatingsMP.RatingsReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Info.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Info.class);
//            job.setOutputFormatClass(TextOutputFormat.class);

            //指定未存在的
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/data/"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

            System.out.println("finished -----");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void chainProcess() {
        try {
            String out1 = "hdfs://localhost:9000/output/job1";
            String out2 = "hdfs://localhost:9000/output/job2";
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/output"));
            Job job1 = Job.getInstance(conf, "rating");
            job1.setJarByClass(Main.class);
            job1.setInputFormatClass(TextInputFormat.class);

            job1.setMapperClass(RatingsMapper.class);
            job1.setReducerClass(RatingsReducer.class);

            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Info.class);

            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Text.class);
//            job.setOutputFormatClass(TextOutputFormat.class);

            //指定未存在的
            FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/input/small/"));
            FileOutputFormat.setOutputPath(job1, new Path(out1));
            job1.waitForCompletion(true);
            System.out.println("job1 finished -----");

            Job job2 = Job.getInstance(conf, " tags");
            job2.setJarByClass(Main.class);
            job2.setInputFormatClass(TextInputFormat.class);

            job2.setMapperClass(UserTagMapper.class);
            job2.setReducerClass(UserTagReducer.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Info.class);

            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(NullWritable.class);
//            job.setOutputFormatClass(TextOutputFormat.class);

            //指定未存在的
            FileInputFormat.addInputPath(job2, new Path(out1));
            FileOutputFormat.setOutputPath(job2, new Path(out2));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);

            System.out.println("finished -----");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
