package edu.berkeley.veloxms;

public class MovieRating {

    private long movieId;
    private long userId;
    private int rating;

    public MovieRating(long userId, long movieId int rating) {
        this.movieId = movieId;
        this.userId = userId;
        this.rating = rating;
    }

    @JsonProperty
    public long getMovieId() {
        return this.movieId;
    }

    @JsonProperty
    public void setMovieId(long m) {
        this.movieId = m;
    }

    @JsonProperty
    public long getUserId() {
        return this.userId;
    }

    @JsonProperty
    public void setUserId(long u) {
        this.userId = u;
    }

    @JsonProperty
    public int getRating() {
        return this.rating;
    }

    @JsonProperty
    public void setRating(int r) {
        this.rating = r;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MovieRating)) {
            return false;
        }
        MovieRating m = (MovieRating) o;
        return (userId == m.userId && movieId == m.movieId);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result*31 + userId;
        result = result*31 + movieId;
        return result;

    }

}
