package edu.berkeley.veloxms;

import tachyon.r.sorted.ClientStore;
import tachyon.TachyonURI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;


/**
 * Stores ratings for all movies and users. Needed to update
 * user models when users rate new movies.
 *
 * TODO: decide whether this becomes source of truth for new ratings, or do new ratings
 * also get sent to a log or something. E.g. do new ratings need to get written to Tachyon
 * for Spark to use to re-run ALS when models get too out of date.
 */
public class RatingsMatrix {

    private static final Logger LOGGER = LoggerFactory.getLogger(RatingsMatrix.class);
    ClientStore ratings;

    // TODO decide best way to store new ratings. For now do simplest thing.
    // threadsafe way to store new ratings
    Set<MovieRating> newRatings;


    public RatingsMatrix(String uri) {
        try {
            ratings = ClientStore.getStore(new TachyonURI(uri));
        } catch (IOException e) {
            LOGGER.error("Exception getting ratings store: " + e.getMessage());
            ratings = null;
        }
        // concurrent hash set
        newRatings = Collections.newSetFromMap(new ConcurrentHashMap<MovieRating, Boolean>());
    }

    public boolean addRating(long userId, long movieId, int rating) {
        return addRating(new MovieRating(userId, movieId, rating);
    }

    // TODO figure out how to do updates later. This is a noop if a rating
    // already exists. Updates difficult because the old rating might be in
    // Tachyon
    public boolean addRating(MovieRating rating) {

        // TODO check if rating exists in tachyon
        // what should the key be in tachyon?
        // if (ratings. 
        return newRatings.add(rating);
    }



}
