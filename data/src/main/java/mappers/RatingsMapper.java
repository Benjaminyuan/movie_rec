package mappers;

import dto.MovieInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingsMapper extends Mapper<LongWritable, Text, LongWritable, MovieInfo> {
    private MovieInfo.MovieInfoBuilder builder = new MovieInfo.MovieInfoBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // input: userId,movieId,ratings,timestamp
        String[] line = value.toString().split(",");
        if ( key.get() == 0||line.length < 4) {
            return;
        }
        MovieInfo info = builder.setMovieId(line[1]).setRating(line[2]).setTimestamp(line[3]).build();
        LongWritable userId = new LongWritable(Long.parseLong(line[0]));
        System.out.println(String.format("map --- userId: %d,movieInfo: %s",userId.get(),info.toString()));
        context.write(userId,info);
    }
}
