package edu.berkeley.veloxms;


import com.fasterxml.jackson.annotation.JsonProperty;

public class MovieRating {

    private long movieId;
    private long userId;
    private float rating;

    public MovieRating(long userId, long movieId, float rating) {
        this.movieId = movieId;
        this.userId = userId;
        this.rating = rating;
    }

    public MovieRating() {
        this.movieId = -1;
        this.userId = -1;
        this.rating = -1.0f;
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
    public float getRating() {
        return this.rating;
    }

    @JsonProperty
    public void setRating(float r) {
        this.rating = r;
    }

    // @Override 
    // public boolean equals(Object o) { 
    //     if (o == this) { 
    //         return true; 
    //     } 
    //     if (!(o instanceof MovieRating)) { 
    //         return false; 
    //     } 
    //     MovieRating m = (MovieRating) o; 
    //     return (userId == m.userId && movieId == m.movieId); 
    // } 
    //
    // @Override 
    // public int hashCode() { 
    //     int result = 17; 
    //     result = result*31 + (int) userId; 
    //     result = result*31 + (int) movieId; 
    //     return result; 
    //
    // } 

}
