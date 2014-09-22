package edu.berkeley.veloxms.storage


import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.lang3.NotImplementedException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tachyon.r.sorted.ClientStore
import scala.util._

import java.io.IOException
import java.util.HashMap
import java.nio.ByteBuffer
import com.typesafe.scalalogging._

class TachyonStorage[U] (
    users: ClientStore,
    items: ClientStore,
    ratings: ClientStore,
    numFactors: Int) extends ModelStorage with LazyLogging {

    def getFeatureData(itemId: Long): Try[U] = {
        // getFactors(itemId, items, "item-model")
        // for {
        //     rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(userId)))
        //     array <- Try(SerializationUtils.deserialize(rawBytes))
        //     result <- Try(array match
        //         case Failure(u) => array
        //         case Success(u) => u.asInstanceOf[U]
        //     )
        // } yield result

        val result = for {
            rawBytes <- Try(items.get(TachyonUtils.long2ByteArr(userId)))
            array <- Try(SerializationUtils.deserialize(rawBytes))
            // result <- Try(array match
            //     case Failure(u) => array
            //     case Success(u) => u.asInstanceOf[U]
            // )
        } yield array
        result match {
            case Success(u) => Success(u.asInstanceOf[U])
            case Failure(u) => array
        }

    }

    def getUserFactors(userId: Long): Try[Array[Double]] = {
        Failure (new NotImplementedException("getUserFactors"))
        // for {
        //     rawBytes <- Try(users.get(TachyonUtils.long2ByteArr(userId)))
        //     array <- Try(SerializationUtils.deserialize(rawBytes))
        //     result <- Try(array match
        //         case Failure(u) => array
        //         case Success(u) => u.asInstanceOf[Array[Double]]
        //     )
        // } yield result
    }

    def getAllObservations(userId: Long): Try[HashMap[Long, Float]] = {
        Failure (new NotImplementedException("getAllObservations"))
        // for {
        //     rawBytes <- Try(ratings.get(TachyonUtils.long2ByteArr(userId)))
        //     array <- Try(SerializationUtils.deserialize(rawBytes))
        //     result <- Try(array match
        //         case Failure(u) => array
        //         case Success(u) => u.asInstanceOf[HashMap[Long, Float]]
        //     )
        // } yield result
    }
}



    //
    // @Override
    // public double[] getUserFactors(long userId) {
    //     return getFactors(userId, users, "user-model");
    // }

    // private static double[] getFactors(id: Long, model: ClientStore, debug: String = "unspecified") {
    //     // ByteBuffer key = ByteBuffer.allocate(8); 
    //     // key.putLong(id); 
    //     try {
    //         byte[] rawBytes = model.get(TachyonUtils.long2ByteArr(id));
    //         if (rawBytes != null) {
    //             return (double[]) SerializationUtils.deserialize(rawBytes);
    //         } else {
    //             LOGGER.warn("no value found in " + debug + " for : " + id);
    //         }
    //     } catch (IOException e) {
    //         LOGGER.warn("Caught tachyon exception: " + e.getMessage());
    //     }
    //     return null;
    // }
    
    // @Override
    // public HashMap<Long, Float> getRatedMovies(long userId) {
    //     HashMap<Long, Float> ratedMovies = null;
    //     try {
    //         LOGGER.info("Looking for ratings for user: " + userId);
    //         byte[] rawBytes = ratings.get(TachyonUtils.long2ByteArr(userId));
    //         if (rawBytes != null) {
    //             ratedMovies = (HashMap<Long, Float>) SerializationUtils.deserialize(rawBytes);
    //         } else {
    //             LOGGER.warn("no value found in ratings for user: " + userId);
    //         }
    //     } catch (IOException e) {
    //         LOGGER.warn("Caught tachyon exception: " + e.getMessage());
    //     }
    //     return ratedMovies;
    // }
    //
    // @Override
    // public int getNumFactors() {
    //     return this.numFactors;
    // }


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

