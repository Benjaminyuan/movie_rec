package dto;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Info implements WritableComparable<Info> {

    private boolean isRatingData;
    private long userId;
    private long movieId;
    private float rating;
    private long timestamp;
    private int tagId;
    private double relevance;

    public Info(MovieInfoBuilder builder) {
        this.isRatingData = builder.isRatingData;
        this.movieId = builder.movieId;
        if (builder.isRatingData) {
            this.userId = builder.userId;
            this.rating = builder.rating;
            this.timestamp = builder.timestamp;
        } else {
            this.tagId = builder.tagId;
            this.relevance = builder.relevance;
        }

    }

    // 不要删除！！
    public Info() {
    }

    public long getUserId() {
        return userId;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public boolean isRatingData() {
        return isRatingData;
    }

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
    public String toString() {
        if (isRatingData) {
            // userId,movieId,rating,timestamp
            return String.format("%d:%d:%f:%d", userId, movieId, rating, timestamp);
        } else {
            // movieId,tagId,relevance
            return String.format("%d:%d:%d", movieId, tagId, relevance);

        }
    }

    @Override
    public int compareTo(Info o) {
        if(userId == o.getUserId()){
            return 0;
        }
        return userId > o.getUserId() ? 1 : -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isRatingData);
        out.writeLong(movieId);
        if (isRatingData) {
            out.writeLong(userId);
            out.writeFloat(rating);
            out.writeLong(timestamp);
        } else {
            out.writeInt(tagId);
            out.writeDouble(relevance);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isRatingData = in.readBoolean();
        this.movieId = in.readLong();
        if (isRatingData) {
            this.userId = in.readLong();
            this.rating = in.readFloat();
            this.timestamp = in.readLong();
        } else {
            this.tagId = in.readInt();
            this.relevance = in.readDouble();
        }
    }

    public static final class MovieInfoBuilder {
        private boolean isRatingData;
        private long movieId;
        private long userId;
        private float rating;
        private long timestamp;
        private int tagId;
        private double relevance;

        public Info build() {
            return new Info(this);
        }

        public MovieInfoBuilder setMovieId(String movieId) {
            this.movieId = Long.parseLong(movieId);
            return this;
        }

        public MovieInfoBuilder setRating(String rating) {
            this.rating = Float.parseFloat(rating);
            return this;
        }

        public MovieInfoBuilder setTimestamp(String timestamp) {
            this.timestamp = Long.parseLong(timestamp);
            return this;
        }

        public MovieInfoBuilder setTagId(String tagId) {
            this.tagId = Integer.parseInt(tagId);
            return this;
        }

        public MovieInfoBuilder setRelevance(String relevance) {
            this.relevance = Float.parseFloat(relevance);
            return this;
        }

        public MovieInfoBuilder setUserId(String userId) {
            this.userId = Long.parseLong(userId);
            return this;
        }

        public MovieInfoBuilder isRatingData(boolean isRatingData) {
            this.isRatingData = isRatingData;
            return this;
        }
    }
}
