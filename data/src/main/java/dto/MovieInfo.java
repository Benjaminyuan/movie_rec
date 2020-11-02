package dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

public class MovieInfo implements WritableComparable<MovieInfo> {
    private long movieId;
    private float rating;
    private long timestamp;

    public MovieInfo(MovieInfoBuilder builder){
        this.movieId = builder.movieId;
        this.rating = builder.rating;
        this.timestamp = builder.timestamp;
    }
    public MovieInfo(){}

    public float getRating() {
        return rating;
    }

    public long getMovieId() {
        return movieId;
    }

    public long getTimestamp() {
        return timestamp;
    }
    @Override
    public String toString(){
        return String.format("%d:%f:%d",movieId,rating,timestamp);
    }

    @Override
    public int compareTo(MovieInfo o) {
        return rating > o.getRating() ? 1 : -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(movieId);
        out.writeFloat(rating);
        out.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movieId = in.readLong();
        this.rating = in.readFloat();
        this.timestamp = in.readLong();
    }

    public static final class MovieInfoBuilder{
        private long movieId;
        private float rating;
        private long timestamp;
        public MovieInfo build(){
            return new MovieInfo(this);
        }
        public MovieInfoBuilder  setMovieId(String movieId){
            this.movieId = Long.parseLong(movieId);
            return this;
        }
        public MovieInfoBuilder setRating(String rating){
            this.rating = Float.parseFloat(rating);
            return this;
        }
        public MovieInfoBuilder setTimestamp(String timestamp){
            this.timestamp = Long.parseLong(timestamp);
            return this;
        }
    }
}
