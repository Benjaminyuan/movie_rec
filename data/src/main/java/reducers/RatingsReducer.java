package reducers;

import dto.MovieInfo;
import hive.HiveConnector;
import hive.HiveOperate;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import service.DataProcess;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class RatingsReducer extends Reducer<LongWritable, MovieInfo, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<MovieInfo> values, Context context) throws IOException, InterruptedException {
        List<MovieInfo> list = new ArrayList<MovieInfo>();
        values.forEach(list::add);
        list.sort((m1, m2) -> {
            float r = m1.getRating() - m2.getRating();
            if (r < 0.1) {
                return 0;
            }
            return r > 0 ? 1 : -1;
        });
        Pair<List<MovieInfo>,List<MovieInfo>> splitUserRatingRawData = DataProcess.splitUserRatingRawData(list);
        HiveOperate.insertUserMovies(splitUserRatingRawData.getFirst(),key.get(),"user_movies");
        HiveOperate.insertUserMovies(splitUserRatingRawData.getFirst(),key.get(),"user_movies_test");
        context.write(key, new Text(list.toString()));
    }
}
