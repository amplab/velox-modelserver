package edu.berkeley.veloxms.storage


import scala.util.Try
import scala.collection.mutable

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

    def getUserFactors(userId: Long): Try[Array[Double]]


    /**
     * Gets a list of all movies this user has rated and the associated ratings.
     */
    def getAllObservations(userId: Long): Try[mutable.HashMap[Long, Float]]

    val numFactors: Int
}


