package edu.berkeley.veloxms.storage;


import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.r.sorted.ClientStore;

import java.io.IOException;
import java.util.HashMap;
import java.nio.ByteBuffer;

public class TachyonStorage implements ModelStorage {

    private final ClientStore users;
    private final ClientStore items;
    private final ClientStore ratings;
    private final ClientStore matPredictions;
    private final int numFactors;

    private static final Logger LOGGER = LoggerFactory.getLogger(TachyonStorage.class);

    // TODO eventually we will want a shared cache among resources
    /* private final ConcurrentHashMap<Long, double[]> itemCache; */
    /* private final ConcurrentHashMap<Long, double[]> userCache; */

    public TachyonStorage(ClientStore users,
                          ClientStore items,
                          ClientStore ratings,
                          ClientStore matPredictions,
                          int numFactors) {
        this.users = users;
        this.items = items;
        this.ratings = ratings;
        this.matPredictions = matPredictions;
        this.numFactors = numFactors;
    }

    @Override
    public double[] getItemFactors(long itemId) {
        return getFactors(itemId, items);
    }

    @Override
    public double[] getUserFactors(long userId) {
        return getFactors(userId, users);
    }

    private static double[] getFactors(long id, ClientStore model) {
        // ByteBuffer key = ByteBuffer.allocate(8); 
        // key.putLong(id); 
        try {
            byte[] rawBytes = model.get(TachyonUtils.long2ByteArr(id));
            return (double[]) SerializationUtils.deserialize(rawBytes);
        } catch (IOException e) {
            LOGGER.warn("Caught tachyon exception: " + e.getMessage());
        }
        return null;
    }
    
    @Override
    public HashMap<Long, Float> getRatedMovies(long userId) {
        HashMap<Long, Float> ratedMovies = null;
        try {
            LOGGER.info("Looking for ratings for user: " + userId);
            byte[] rawBytes = ratings.get(TachyonUtils.long2ByteArr(userId));
            if (rawBytes != null) {
                ratedMovies = (HashMap<Long, Float>) SerializationUtils.deserialize(rawBytes);
            } else {
                LOGGER.warn("no value found in ratings for user: " + userId);
            }
        } catch (IOException e) {
            LOGGER.warn("Caught tachyon exception: " + e.getMessage());
        }
        return ratedMovies;
    }

    @Override
    public int getNumFactors() {
        return this.numFactors;
    }


    // TODO deicde if a single KV pair per prediction is the best way to do this
    @Override
    public double getMaterializedPrediction(long userId, long movieId) {
        double prediction = -1.0;
        try {
            byte[] rawPrediction =
                matPredictions.get(TachyonUtils.twoDimensionKey(userId, movieId));
            if (rawPrediction != null) {
                prediction = ByteBuffer.wrap(rawPrediction).getDouble();
            }
        } catch (IOException e) {
            LOGGER.warn("Caught tachyon exception: " + e.getMessage());
        }
        return prediction;
    }
}

