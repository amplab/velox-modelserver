package edu.berkeley.veloxms.storage


import scala.util.Try
// import scala.collection.immutable.HashMap

/**
 * Simple interface to abstract out the KV storage backend used to store
 * the models from the application logic to access them.
 *
 * @tparam U The type of the data being stored in the KV store to
 * comput features
 */
trait ModelStorage[U] {

    /**
     * Get factor vector for provided item
     * @param itemId the item id to get
     */
    def getFeatureData(itemId: Long): Try[U]

    def getUserFactors(userId: Long): Try[WeightVector]


    /**
     * Gets a list of all movies this user has rated and the associated ratings.
     */
    def getAllObservations(userId: Long): Try[Map[Long, Double]]

    val numFactors: Int
}


