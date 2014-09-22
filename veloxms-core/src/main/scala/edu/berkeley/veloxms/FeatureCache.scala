/**
 * Caches features on a per-item basis. Basically a simple wrapper for
 * thread-safe hashmap.
 */
package edu.berkeley.veloxms.resources

import java.util.concurrent.ConcurrentHashMap

class FeatureCache(budget: Int) {

  // TODO: maybe we should universally represent features in JBLAS format
  // so I don't have to keep transforming them between vectors and arrays
  private val cache = new ConcurrentHashMap[Long, Array[Double]]()

  def addItem(data: Array[Byte], features: Array[Double]): Boolean = {

  }

  def getItem(data: Array[Byte]): Option[Array[Double]] = {

  }



}
