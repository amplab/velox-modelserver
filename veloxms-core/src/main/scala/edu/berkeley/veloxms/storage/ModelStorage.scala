package edu.berkeley.veloxms.storage


import scala.util.Try

/**
 * Simple interface to abstract out the KV storage backend used to store
 * the models from the application logic to access them.
 */
trait ModelStorage[U] {

    /**
     * Get factor vector for provided item
     * @param itemId the item id to get
     */
    def getFeatureData(itemId: Long): Try[U]

    def getUserFactors(userId: Long): Try[Array[Double]]


    /**
     * Gets a list of all movies this user has rated and the associated ratings.
     */
    def getAllObservations(userId: Long): Try[HashMap[Long, Float]]

    def getNumFactors(): Int
}


