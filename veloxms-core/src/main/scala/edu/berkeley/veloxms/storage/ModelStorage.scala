package edu.berkeley.veloxms.storage


import io.dropwizard.lifecycle.Managed

import scala.util.Try
import edu.berkeley.veloxms._

// import scala.collection.immutable.HashMap

/**
 * Simple interface to abstract out the KV storage backend used to store
 * the models from the application logic to access them.
 *
 * @tparam U The type of the data being stored in the KV store to
 *           compute features
 */
trait ModelStorage[U] extends Managed{


    /**
     * Lookup item-specific data.
     * This could be pre-computed features or per-item metadata
     * needed for computing features (e.g. human-tagged genres)
     * @param itemId the unique ID of the item
     */
    def getFeatureData(itemId: Long): Try[U]

    /**
     * Lookup existing ensemble weighting for the provided user
     * @param userId The unique ID of the user
     */
    def getUserFactors(userId: Long): Try[WeightVector]


    /**
     * Get a map of all training data associated with this user.
     * @param userId the unique ID of the user
     * @return A map of (itemId -> score) pairs, s.t. for each pair
     * user userId has previously rated item itemId with rating score
     */
    def getAllObservations(userId: Long): Try[Map[Long, Double]]

    /**
     * Add an observation to the training data.
     * @param userId the unique ID of the user
     * @param itemId the unique ID of the item that observation was made on
     * @param score the observation made
     */
    def addScore(userId: Long, itemId: Long, score: Double)

    /** The number of top-level features in use (this is the dimension of the
     * user's weight vector)
     */
    val numFactors: Int

  override def start() { }
}


