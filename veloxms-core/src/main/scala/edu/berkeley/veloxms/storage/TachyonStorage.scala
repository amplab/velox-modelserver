package edu.berkeley.veloxms.storage


import org.apache.commons.lang3.SerializationUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tachyon.r.sorted.ClientStore

import java.io.IOException
import java.util.HashMap
import java.nio.ByteBuffer

class TachyonStorage(
    users: ClienStore,
    items: ClientStore,
    ratings: ClientStore,
    numFactors: Int) extends ModelStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(TachyonStorage.class);

    override def getItemFactors(itemId: Long): Array[Double] = {
        getFactors(itemId, items, "item-model");
    }

    override def getUserFactors(userId: Long): Array[Double] = {
        getFactors(userId, users, "item-model");
    }

    @Override
    public double[] getUserFactors(long userId) {
        return getFactors(userId, users, "user-model");
    }

    private static double[] getFactors(id: Long, model: ClientStore, debug: String = "unspecified") {
        // ByteBuffer key = ByteBuffer.allocate(8); 
        // key.putLong(id); 
        try {
            byte[] rawBytes = model.get(TachyonUtils.long2ByteArr(id));
            if (rawBytes != null) {
                return (double[]) SerializationUtils.deserialize(rawBytes);
            } else {
                LOGGER.warn("no value found in " + debug + " for : " + id);
            }
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
    // @Override 
    // public double getMaterializedPrediction(long userId, long movieId) { 
    //     double prediction = -1.0; 
    //     try { 
    //         byte[] rawPrediction = 
    //             matPredictions.get(TachyonUtils.twoDimensionKey(userId, movieId)); 
    //         if (rawPrediction != null) { 
    //             prediction = ByteBuffer.wrap(rawPrediction).getDouble(); 
    //         } 
    //     } catch (IOException e) { 
    //         LOGGER.warn("Caught tachyon exception: " + e.getMessage()); 
    //     } 
    //     return prediction; 
    // } 
}

