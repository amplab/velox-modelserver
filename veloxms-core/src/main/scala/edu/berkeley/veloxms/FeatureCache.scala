/**
 * Caches features on a per-item basis. Basically a simple wrapper for
 * thread-safe hashmap.
 */
package edu.berkeley.veloxms

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * @tparam K type of the item whose features are being stored.
 */
// TODO make sure that K is hashable
class FeatureCache[K, V](cacheActivated: Boolean) {

  // TODO: maybe we should universally represent features in JBLAS format
  // so I don't have to keep transforming them between vectors and arrays
  private val _cache = new ConcurrentHashMap[K, V]()
  private val hits = new AtomicLong()
  private val misses = new AtomicLong()



  def addItem(data: K, features: V) {
    if (cacheActivated) {
      _cache.putIfAbsent(data, features)
    }
  }

  def getItem(data: K): Option[V] = {
    if (cacheActivated) {
      val result = Option(_cache.get(data))
      result match {
        case Some(_) => hits.incrementAndGet()
        case None => misses.incrementAndGet()
      }
      result
    } else {
      misses.incrementAndGet()
      None
    }
  }

  def getCacheHitRate: (Long, Long) = {
    (hits.get(), misses.get())
  }
}


// object FeatureCache {
//
//   // Totally arbitrary placeholder until we figure out what
//   // a cache budget means
//   val tempBudget = 100
//
// }
