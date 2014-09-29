/**
 * Caches features on a per-item basis. Basically a simple wrapper for
 * thread-safe hashmap.
 */
package edu.berkeley.veloxms

import java.util.concurrent.ConcurrentHashMap

/**
 * @tparam T type of the item whose features are being stored.
 */
// TODO make sure that T is hashable
class FeatureCache[T](budget: Int) {

  // TODO: maybe we should universally represent features in JBLAS format
  // so I don't have to keep transforming them between vectors and arrays
  private val cache = new ConcurrentHashMap[Long, Array[Double]]()

  def addItem(data: T, features: Array[Double]): Boolean = {
    // TODO implement
    false
  }

  def getItem(data: T): Option[Array[Double]] = {
    // TODO implement
    None
  }



}


object FeatureCache {

  // Totally arbitrary placeholder until we figure out what
  // a cache budget means
  val tempBudget = 100

}
