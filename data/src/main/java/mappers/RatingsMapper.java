package mappers;

import dto.Info;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class RatingsMapper extends Mapper<LongWritable, Text, LongWritable, Info> {
    private Info.MovieInfoBuilder builder = new Info.MovieInfoBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        }
        // input: userId,movieId,ratings,timestamp
        String[] line = value.toString().split(",");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();

        Info info = null;
        LongWritable movieId = null;
        // 长度判断防止越界
        Configuration conf = context.getConfiguration();
        if (name.contains("ratings") && line.length >= 4) {
            // userId,movieId,rating,timestamp
            movieId = new LongWritable(Long.parseLong(line[1]));
            info = builder.isRatingData(true)
                    .setMovieId(line[1])
                    .setUserId(line[0])
                    .setRating(line[2])
                    .setTimestamp(line[3])
                    .build();

        } else if (name.contains("scores") && line.length >= 3) {
            // movieId,tagId,relevance
            movieId = new LongWritable(Long.parseLong(line[0]));
            info = builder.isRatingData(false).setMovieId(line[0])
                    .setTagId(line[1])
                    .setRelevance(line[2])
                    .build();

        } else {
            return;
        }
        context.write(movieId, info);
    }
}
