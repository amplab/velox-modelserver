package edu.berkeley.veloxms.storage


import java.util.HashMap

/**
 * Simple interface to abstract out the KV storage backend used to store
 * the models from the application logic to access them.
 */
trait ModelStorage {

    /**
     * Get factor vector for provided item
     * @param itemId the item id to get
     */
    def getItemFactors(itemId: Long): Array[Double]
    def getUserFactors(userId: Long): Array[Double]


    /**
     * Gets a list of all movies this user has rated and the associated ratings.
     */
    def getAllObservations(userId: Long): HashMap[Long, Float]

    def getNumFactors(): Int
}


